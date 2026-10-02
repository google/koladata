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
#include "py/koladata/base/py_proto_utils.h"

#include <any>
#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
#include "arolla/util/status_macros_backport.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/unit.h"
#include "koladata/data_bag.h"
#include "koladata/data_slice.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/dtype.h"
#include "koladata/internal/slice_builder.h"
#include "koladata/operators/slices.h"
#include "koladata/proto/from_proto.h"
#include "google/protobuf/message.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/koladata/base/pybind11_protobuf_wrapper.h"

namespace koladata::python {
absl::StatusOr<DataSlice> FromProtoObjects(
    const absl_nonnull DataBagPtr& db, const std::vector<PyObject*>& py_objects,
    absl::Span<const absl::string_view> extensions,
    const std::optional<DataSlice>& itemid,
    const std::optional<DataSlice>& schema) {
  arolla::python::DCheckPyGIL();

  // Hold strong references to the Python proto objects in the outer function
  // scope. They will increment the reference count and prevent the Python
  // garbage collector from deallocating them when the GIL is released
  // temporarily in the block below.
  std::vector<arolla::python::PyObjectPtr> proto_holders;
  proto_holders.reserve(py_objects.size());

  internal::SliceBuilder message_mask_builder(py_objects.size());
  auto typed_message_mask_builder = message_mask_builder.typed<arolla::Unit>();
  std::vector<std::any> message_owners;
  message_owners.reserve(py_objects.size());
  std::vector<const ::google::protobuf::Message* absl_nonnull> message_ptrs;
  message_ptrs.reserve(py_objects.size());

  for (size_t i = 0; i < py_objects.size(); ++i) {
    PyObject* py_message = py_objects[i];
    if (py_message != Py_None) {
      // INCREF: guarantees the Python proto cannot be deallocated by another
      // thread.
      proto_holders.push_back(arolla::python::PyObjectPtr::NewRef(py_message));
      typed_message_mask_builder.InsertIfNotSet(i, arolla::kUnit);

      // Note: `message_owner` is a `std::any` holding a
      // `pybind11::detail::type_caster`. This and all future implementations
      // for the `std::any` handle should not keep the Python object alive,
      // which is why `proto_holders` is needed.
      ASSIGN_OR_RETURN((auto [message_ptr, message_owner]),
                       python::UnwrapPyProtoMessage(py_message));
      message_owners.push_back(std::move(message_owner));
      message_ptrs.push_back(message_ptr);
    }
  }

  ASSIGN_OR_RETURN(
      auto message_mask,
      DataSlice::Create(std::move(message_mask_builder).Build(),
                        DataSlice::JaggedShape::FlatFromSize(py_objects.size()),
                        internal::DataItem(schema::kMask)));

  {
    // When exiting this block, `~ReleasePyGIL()` runs first to restore the GIL
    // before `proto_holders` (in the outer scope) destructs and calls
    // `Py_DECREF`.
    arolla::python::ReleasePyGIL release_gil;
    ASSIGN_OR_RETURN(DataSlice dense_result,
                     FromProto(db, message_ptrs, extensions, itemid, schema));
    return ops::InverseSelect(dense_result, message_mask);
  }
}
}  // namespace koladata::python
