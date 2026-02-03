// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef KOLADATA_DATA_BAG_H_
#define KOLADATA_DATA_BAG_H_

#include <atomic>
#include <memory>
#include <string>
#include <typeindex>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/pseudo_random.h"

namespace koladata {

class DataBag;

using DataBagPtr = arolla::RefcountPtr<DataBag>;

namespace s11n {
struct DataBagDecoder;
}  // namespace s11n

// This abstraction implements the API of all public DataBag functionality
// users can access. It is used as the main entry point to business logic
// implementation and all the processing is delegated to it from C Python
// bindings for DataBag.
//
// C Python bindings for DataBag is processing only the minimum part necessary
// to extract information from PyObject(s) and propagate it to appropriate
// methods of this class.
//
// In addition, it provides indirection from the low-level DataBagImpl, so that
// the underlying Object storage can be changed for many DataSlice(s). This way
// full persistency can be achieved with partially persistent DataBagImpl.
class DataBag : public arolla::RefcountedBase {
 public:
  // Tag for creating immutable DataBag.
  struct immutable_t {};

  // Returns a newly created empty mutable DataBag.
  static DataBagPtr EmptyMutable() { return DataBagPtr::Make(); }

  // Returns a newly created empty immutable DataBag.
  static DataBagPtr Empty() { return DataBagPtr::Make(immutable_t()); }

  DataBag() : DataBag(/*is_mutable=*/true) {}
  explicit DataBag(immutable_t) : DataBag(/*is_mutable=*/false) {}
  ~DataBag() noexcept;

  bool IsMutable() const { return is_mutable_; }

  const internal::DataBagImpl& GetImpl() const { return *impl_; }
  absl::StatusOr<internal::DataBagImpl&> GetMutableImpl() {
    if (!is_mutable_) {
      return absl::InvalidArgumentError(
          "cannot modify/create item(s) on an immutable DataBag, perhaps use "
          "immutable update APIs (e.g. with_attr, with_dict_update) on "
          "immutable entities/dicts or use db.fork() or ds.fork_bag() create a "
          "mutable DataBag first");
    }
    // We check forked_ first without exchanging to be more efficient when it is
    // already false (which is the common case).
    if (forked_ && forked_.exchange(false)) {
      impl_ = impl_->PartiallyPersistentFork();
      impl_->AssignToDataBag();
    }
    return *impl_;
  }

  // Returns a newly created mutable DataBag with the same content as this one.
  // Changes to either DataBag will not be reflected in the other.
  absl::StatusOr<DataBagPtr> Fork(bool immutable = false);

  // Creates an immutable fork of the DataBag. Supports DataBags with fallbacks.
  DataBagPtr Freeze();

  // Makes the current DataBag immutable.
  //
  // Use this function with caution because if the data bag is shared between
  // several users, some of them might not expect that it suddenly becomes
  // immutable.
  void UnsafeMakeImmutable() { is_mutable_ = false; }

  // Returns fallbacks in priority order.
  const std::vector<DataBagPtr>& GetFallbacks() const { return fallbacks_; }

  // Returns a newly created immutable DataBag with fallbacks.
  // An error is returned if fallbacks are mutable.
  static absl::StatusOr<DataBagPtr> ImmutableEmptyWithFallbacks(
      absl::Span<const DataBagPtr absl_nullable> fallbacks);

  // Returns a new DataBag with all the fallbacks merged.
  absl::StatusOr<DataBagPtr> MergeFallbacks();

  // Merge additional attributes and objects from `other_db`.
  // When allow_data_conflicts is true, the data is merged even if there is a
  // conflict, when allow_data_conflicts is false, we raise on a data conflict.
  // When allow_schema_conflicts is true, the explicit schemas are merged even
  // if there is a conflict, and when it is false, we raise on an explicit
  // schema conflict. The value of the overwrite argument tells whether the new
  // or the old value is kept in case of a conflict when conflicts are allowed.
  absl::Status MergeInplace(const DataBagPtr& other_db, bool overwrite,
                            bool allow_data_conflicts,
                            bool allow_schema_conflicts);

  // Fingerprint of the DataBag (randomized).
  arolla::Fingerprint fingerprint() const { return fingerprint_; }

  // Returns metadata assigned to this DataBag instance via `SetCachedMetadata`,
  // or `nullptr` if none is present. Note that DataBag forking and other
  // operations that produce a new DataBag never inherit this cache. "Mutable"
  // databags do not support caching, and this method will always return
  // `nullptr` for them.
  template <typename T>
  std::shared_ptr<const T> absl_nullable GetCachedMetadataOrNull(
      internal::ObjectId key) ABSL_ATTRIBUTE_LIFETIME_BOUND {
    absl::MutexLock lock(cache_mutex_);
    auto it = cache_.find({key, typeid(T)});
    if (it == cache_.end()) {
      return nullptr;
    }
    return std::static_pointer_cast<const T>(it->second);
  }

  // Metadata cache is a map (ObjectId, typeid(T)) => shared_ptr<T> attached
  // to every immutable DataBag. Values of any movable type can be attached by
  // `SetCachedMetadata` and requested by `GetCachedMetadataOrNull`.
  // Returns pointer to the stored data or an error status if the data bag is
  // mutable.
  // Needed for performance optimizations - to cache data which can be obtained
  // from data bag content, but takes time to obtain.
  //
  // * Values can be cached only if the data bag is immutable and has no mutable
  //     fallbacks. In a mutable data bag the cache is always empty and
  //     `GetCachedMetadataOrNull` returns nullptr.
  //
  // * In case of mutable databag or mutable fallbacks `SetCachedMetadata`
  //     forwards the value without storing it in the cache.
  //
  // * Data is not overridable: `SetCachedMetadata(key, value)` is no-op if
  //     a value of this type is already added for this key (in this case
  //     returns a pointer to the old value). It is supposed to be used only for
  //     data that never change.
  //
  // * SetCachedMetadata/GetCachedMetadataOrNull are thread-safe. Note that if
  //     SetCachedMetadata is called with the same key and type simultaniously
  //     from several threads, then the first call wins, and others will return
  //     pointers to the data stored by the first call.
  //
  // * Metadata cache is ignored during serialization and is not preserved when
  //     forking a data bag.
  //
  // * In order to avoid ownership loops don't store references to data bags in
  //     the cache.
  //
  template <typename T>
  std::shared_ptr<const T> SetCachedMetadata(internal::ObjectId key,
                                             std::shared_ptr<const T> v) {
    if (IsMutable()) {
      return v;
    }
    absl::MutexLock lock(cache_mutex_);
    auto& c = cache_[{key, typeid(T)}];
    // No override if already present.
    if (c == nullptr) {
      c = std::move(v);
    }
    return std::static_pointer_cast<const T>(c);
  }

  template <typename T>
  std::shared_ptr<const T> SetCachedMetadata(internal::ObjectId key, T v) {
    return SetCachedMetadata<T>(key, std::make_shared<T>(std::move(v)));
  }

 private:
  friend ::koladata::s11n::DataBagDecoder;

  explicit DataBag(bool is_mutable)
      : impl_(internal::DataBagImpl::CreateEmptyDatabag()),
        is_mutable_(is_mutable),
        // NOTE: consider lazy initialization of the fingerprint if it becomes
        // expensive to compute.
        fingerprint_(internal::PseudoRandomFingerprint()) {}

  // Returns a mutable DataBag that wraps provided low-level DataBagImpl.
  static DataBagPtr FromImpl(internal::DataBagImplPtr impl);

  // Returns a newly created mutable DataBag with the same content as this one.
  // Changes to either DataBag will not be reflected in the other. Assumes
  // DataBag has no fallbacks.
  DataBagPtr FallbackFreeFork(bool immutable = false);

  internal::DataBagImplPtr impl_;
  std::vector<DataBagPtr> fallbacks_;
  bool is_mutable_;
  arolla::Fingerprint fingerprint_;

  // Used to implement lazy forking for immutable DataBags.
  std::atomic<bool> forked_ = false;

  absl::Mutex cache_mutex_;
  absl::flat_hash_map<std::pair<internal::ObjectId, std::type_index>,
                      std::shared_ptr<const void>>
      cache_ ABSL_GUARDED_BY(cache_mutex_);
};

// Call visit_fn on all fallbacks in pre-order DFS.
void VisitFallbacks(const DataBag& bag,
                    absl::FunctionRef<void(DataBagPtr)> visit_fn);

class FlattenFallbackFinder {
 public:
  // Constructs empty fallback list.
  FlattenFallbackFinder() = default;

  // Constructs fallback list from the provided databag.
  explicit FlattenFallbackFinder(const DataBag& bag) {
    if (bag.GetFallbacks().empty()) {
      return;
    }

    flattened_fallbacks_.reserve(bag.GetFallbacks().size());
    VisitFallbacks(bag, [this](DataBagPtr fallback) {
      this->flattened_fallbacks_.push_back(&fallback->GetImpl());
    });
  }

  // Returns DatBagImpl fallbacks in the decreasing priority order.
  // All duplicates are removed.
  // The returned span is valid as long as the FlattenFallbackFinder is alive.
  internal::DataBagImpl::FallbackSpan GetFlattenFallbacks() const {
    return flattened_fallbacks_;
  }

 private:
  absl::InlinedVector<const internal::DataBagImpl*, 2> flattened_fallbacks_;
};

// Returns the string representation of the DataBag.
std::string GetBagIdRepr(const DataBagPtr& db);

}  // namespace koladata

namespace arolla {

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(::koladata::DataBagPtr);
AROLLA_DECLARE_REPR(::koladata::DataBagPtr);
AROLLA_DECLARE_SIMPLE_QTYPE(DATA_BAG, ::koladata::DataBagPtr);

}  // namespace arolla

#endif  // KOLADATA_DATA_BAG_H_
