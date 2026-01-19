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
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/nullability.h"
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
#include "py/koladata/types/pybind11_protobuf_wrapper.h"
#include "arolla/util/status_macros_backport.h"

namespace koladata::python {
absl::StatusOr<DataSlice> FromProtoObjects(
    const absl_nonnull DataBagPtr& db, const std::vector<PyObject*>& py_objects,
    absl::Span<const absl::string_view> extensions,
    const std::optional<DataSlice>& itemid,
    const std::optional<DataSlice>& schema) {
  arolla::python::DCheckPyGIL();
  const Py_ssize_t messages_list_len = py_objects.size();

  internal::SliceBuilder message_mask_builder(messages_list_len);
  auto typed_message_mask_builder = message_mask_builder.typed<arolla::Unit>();
  std::vector<std::any> message_owners;
  message_owners.reserve(messages_list_len);
  std::vector<const ::google::protobuf::Message* absl_nonnull> message_ptrs;
  message_ptrs.reserve(messages_list_len);
  for (Py_ssize_t i = 0; i < messages_list_len; ++i) {
    PyObject* py_message = py_objects[i];  // Borrowed.
    if (py_message != Py_None) {
      typed_message_mask_builder.InsertIfNotSet(i, arolla::kUnit);

      ASSIGN_OR_RETURN((auto [message_ptr, message_owner]),
                       python::UnwrapPyProtoMessage(py_message));
      message_owners.push_back(std::move(message_owner));
      message_ptrs.push_back(message_ptr);
    }
  }
  ASSIGN_OR_RETURN(
      auto message_mask,
      DataSlice::Create(std::move(message_mask_builder).Build(),
                        DataSlice::JaggedShape::FlatFromSize(messages_list_len),
                        internal::DataItem(schema::kMask)));

  ASSIGN_OR_RETURN(DataSlice dense_result,
                   FromProto(db, message_ptrs, extensions, itemid, schema));
  ASSIGN_OR_RETURN(DataSlice result,
                   ops::InverseSelect(dense_result, message_mask));
  return result;
}
}  // namespace koladata::python
