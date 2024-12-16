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
#include "koladata/data_slice_qtype.h"

#include <string>

#include "absl/base/no_destructor.h"
#include "absl/strings/string_view.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/data_slice_repr.h"
#include "koladata/internal/data_item.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"

namespace arolla {

using ::koladata::DataBagPtr;
using ::koladata::DataSlice;

QTypePtr QTypeTraits<DataSlice>::type() {
  struct DataSliceQType final : SimpleQType {
    DataSliceQType() : SimpleQType(meta::type<DataSlice>(), "DATA_SLICE") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* source) const final {
      return static_cast<const DataSlice*>(source)
          ->py_qvalue_specialization_key();
    }
  };
  static const absl::NoDestructor<DataSliceQType> result;
  return result.get();
}

void FingerprintHasherTraits<DataSlice>::operator()(FingerprintHasher* hasher,
                                                    const DataSlice& ds) const {
  ds.VisitImpl([&]<class T>(const T& impl) { hasher->Combine(impl); });
  hasher->Combine(ds.GetShape());
  hasher->Combine(ds.GetSchemaImpl().StableFingerprint());
  // WARNING: Fingerprint must always be the same, even if the object is
  // mutated, because it is a TypedValue. This means that Fingerprint of `db_`
  // should remain the same, even if its contents change or we should compute
  // it statically.
  if (ds.GetBag() != nullptr) {
    hasher->Combine(ds.GetBag());
  }
}

ReprToken ReprTraits<DataSlice>::operator()(const DataSlice& value) const {
  return ReprToken{DataSliceRepr(value, {.show_attributes = true,
                                         .show_databag_id = false,
                                         .show_shape = false})};
}

}  // namespace arolla
