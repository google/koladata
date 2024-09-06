// Copyright 2024 Google LLC
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
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "koladata/internal/data_bag.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"

namespace koladata {

class DataBag;

using DataBagPtr = std::shared_ptr<DataBag>;

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
class DataBag {
 public:
  // Tag for creating immutable DataBag.
  struct immutable_t {};

  // Returns a newly created empty DataBag.
  static DataBagPtr Empty() { return std::make_shared<DataBag>(); }

  DataBag() : DataBag(/*is_mutable=*/true) {}
  explicit DataBag(immutable_t) : DataBag(/*is_mutable=*/false) {}

  bool IsMutable() const { return is_mutable_; }

  const internal::DataBagImpl& GetImpl() const { return *impl_; }
  absl::StatusOr<std::reference_wrapper<internal::DataBagImpl>>
  GetMutableImpl() {
    if (!is_mutable_) {
      return absl::InvalidArgumentError(
          "DataBag is immutable, try DataSlice.fork_db()");
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

  // Returns a new immutable DataBag with contents moved from this one. This
  // DataBag is invalidated.
  DataBagPtr ToImmutable() && {
    auto new_db = std::make_shared<DataBag>(immutable_t{});
    new_db->impl_ = std::move(impl_);
    new_db->fallbacks_ = std::move(fallbacks_);
    return new_db;
  }

  // Returns fallbacks in priority order.
  const std::vector<DataBagPtr>& GetFallbacks() const { return fallbacks_; }

  // Returns a newly created immutable DataBag with fallbacks.
  static DataBagPtr ImmutableEmptyWithFallbacks(
      absl::Span<const DataBagPtr> fallbacks);

  // Returns a DataBag that contains all the data its input contain.
  // * If they are all the same or only 1 DataBag is non-nullptr, that DataBag
  //   is returned.
  // * Otherwise, an immutable DataBag with all the inputs as fallbacks is
  //   created and returned.
  // * In case of no DataBags, nullptr is returned.
  static DataBagPtr CommonDataBag(absl::Span<const DataBagPtr> databags);

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

 private:
  explicit DataBag(bool is_mutable)
      : impl_(internal::DataBagImpl::CreateEmptyDatabag()),
        is_mutable_(is_mutable),
        // NOTE: consider lazy initialization of the fingerprint if it becomes
        // expensive to compute.
        fingerprint_(arolla::RandomFingerprint()) {}

  // Returns a mutable DataBag that wraps provided low-level DataBagImpl.
  static DataBagPtr FromImpl(internal::DataBagImplPtr impl);

  internal::DataBagImplPtr impl_;
  std::vector<DataBagPtr> fallbacks_;
  bool is_mutable_;
  arolla::Fingerprint fingerprint_;

  // Used to implement lazy forking for immutable DataBags.
  std::atomic<bool> forked_ = false;
};

class FlattenFallbackFinder {
 public:
  // Constructs empty fallback list.
  FlattenFallbackFinder() = default;

  // Constructs fallback list from the provided databag.
  FlattenFallbackFinder(const DataBag& bag) {
    const auto& fallbacks = bag.GetFallbacks();
    if (fallbacks.empty()) {
      return;
    }
    CollectFlattenFallbacks(bag, fallbacks);
  }

  // Returns DatBagImpl fallbacks in the decreasing priority order.
  // All duplicates are removed.
  internal::DataBagImpl::FallbackSpan GetFlattenFallbacks() const {
    return fallback_span_;
  }

 private:
  // Collect fallbacks in pre order using Depth First Search.
  void CollectFlattenFallbacks(const DataBag& bag,
                               const std::vector<DataBagPtr>& fallbacks);

  absl::InlinedVector<const internal::DataBagImpl*, 2> fallback_holder_;
  internal::DataBagImpl::FallbackSpan fallback_span_;
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
