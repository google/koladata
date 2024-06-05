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
#ifndef KOLADATA_DATA_BAG_COMPARISON_H_
#define KOLADATA_DATA_BAG_COMPARISON_H_

#include "koladata/data_bag.h"
#include "koladata/internal/triples.h"


namespace koladata {

class DataBagComparison {
 public:
  static bool ExactlyEqual(const DataBagPtr a, const DataBagPtr b) {
    using Triples = internal::debug::Triples;
    if (Triples(a->GetImpl().ExtractContent().value()) !=
        Triples(b->GetImpl().ExtractContent().value())) {
      return false;
    }
    FlattenFallbackFinder a_fb_finder(*a);
    FlattenFallbackFinder b_fb_finder(*b);
    const auto& a_fallbacks = a_fb_finder.GetFlattenFallbacks();
    const auto& b_fallbacks = b_fb_finder.GetFlattenFallbacks();
    if (a_fallbacks.size() != b_fallbacks.size()) {
      return false;
    }
    for (int i = 0; i < a_fallbacks.size(); ++i) {
      if (Triples(a_fallbacks[i]->ExtractContent().value()) !=
          Triples(b_fallbacks[i]->ExtractContent().value())) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace koladata

#endif  // KOLADATA_DATA_BAG_COMPARISON_H_
