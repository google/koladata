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
#include "koladata/internal/triples.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "koladata/internal/data_bag.h"
#include "koladata/internal/data_item.h"
#include "koladata/internal/data_slice.h"
#include "koladata/internal/object_id.h"
#include "koladata/internal/testing/matchers.h"
#include "koladata/internal/uuid_object.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/text.h"

namespace koladata::internal::debug {
namespace {

using ::koladata::internal::testing::DataBagEqual;
using ::testing::Not;

TEST(TriplesTest, TripleDebugString) {
  ObjectId obj = Allocate(15).ObjectByOffset(3);
  ObjectId dict = AllocateDicts(15).ObjectByOffset(3);
  ObjectId schema = AllocateExplicitSchema();
  EXPECT_THAT((AttrTriple{obj, "a", DataItem(1)}).DebugString(),
              ::testing::MatchesRegex(
                  "ObjectId=Entity:\\$[0-9a-zA-Z]{22} attr=a value=1"));
  EXPECT_THAT((DictItemTriple{dict, DataItem(arolla::Text("a")), DataItem(1)})
                  .DebugString(),
              ::testing::MatchesRegex(
                  "DictId=Dict:\\$[0-9a-zA-Z]{22} key='a' value=1"));
  EXPECT_THAT((DictItemTriple{schema, DataItem(arolla::Text("a")), DataItem(1)})
                  .DebugString(),
              ::testing::MatchesRegex(
                  "SchemaId=Schema:\\$[0-9a-zA-Z]{22} key='a' value=1"));
}

TEST(TriplesTest, SimpleAttr) {
  ObjectId obj1 = CreateUuidObject(arolla::Fingerprint(1));
  ObjectId obj2 = CreateUuidObject(arolla::Fingerprint(2));

  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetAttr(DataItem(obj1), "a", DataItem(obj2)));
  ASSERT_OK(db->SetAttr(DataItem(obj2), "b", DataItem(5)));
  ASSERT_OK(db->SetAttr(DataItem(obj1), "c", DataItem(arolla::Text("aaa"))));
  ASSERT_OK(db->SetAttr(DataItem(obj2), "c", DataItem(arolla::Bytes("bbb"))));
  ASSERT_OK(db->SetAttr(DataItem(obj2), "d", DataItem()));

  EXPECT_EQ(Triples(*db->ExtractContent()).DebugString(), R"DB(DataBag {
  ObjectId=Entity:#07Xy61DuGvoVPxWFT5JFKK attr=a value=Entity:#07Xy61DuGvorOdnpUBTWsa
  ObjectId=Entity:#07Xy61DuGvoVPxWFT5JFKK attr=c value='aaa'
  ObjectId=Entity:#07Xy61DuGvorOdnpUBTWsa attr=b value=5
  ObjectId=Entity:#07Xy61DuGvorOdnpUBTWsa attr=c value=b'bbb'
  ObjectId=Entity:#07Xy61DuGvorOdnpUBTWsa attr=d value=None
})DB");
}

TEST(TriplesTest, SimpleDict) {
  DataItem dict = DataItem(AllocateSingleDict());
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->SetInDict(dict, DataItem(3), DataItem(4)));
  ASSERT_OK(db->SetInDict(dict, DataItem(1), DataItem(2)));
  DataBagIndex index;

  // Empty index -> empty content.
  EXPECT_EQ(Triples(*db->ExtractContent(index)).DebugString(), "DataBag {\n}");

  // Request attr that doesn't exist in `db` (should be ignored).
  index.attrs.emplace("a", DataBagIndex::AttrIndex{{Allocate(5)}});

  // Request content of `dict`.
  index.dicts.push_back(AllocationId(dict.value<ObjectId>()));

  EXPECT_THAT(Triples(*db->ExtractContent(index)).DebugString(),
              ::testing::MatchesRegex(R"DB(DataBag \{
  DictId=Dict:\$[0-9a-zA-Z]{22} key=1 value=2
  DictId=Dict:\$[0-9a-zA-Z]{22} key=3 value=4
\})DB"));

  EXPECT_THAT(db, DataBagEqual(db));
  EXPECT_THAT(db, Not(DataBagEqual(DataBagImpl::CreateEmptyDatabag())));
}

TEST(TriplesTest, SimpleList) {
  DataItem list1 = DataItem(AllocateSingleList());
  DataItem list2 = DataItem(AllocateSingleList());
  auto db = DataBagImpl::CreateEmptyDatabag();
  ASSERT_OK(db->AppendToList(list1, DataItem(5)));
  ASSERT_OK(db->AppendToList(list2, DataItem(4)));
  ASSERT_OK(db->AppendToList(list1, DataItem(3)));
  EXPECT_EQ(Triples(*db->ExtractContent(DataBagIndex())).DebugString(),
            "DataBag {\n}");
  EXPECT_THAT(Triples(*db->ExtractContent()).DebugString(),
              ::testing::MatchesRegex(R"DB(DataBag \{
  ListId=List:\$[0-9a-zA-Z]{22} \[5, 3\]
  ListId=List:\$[0-9a-zA-Z]{22} \[4\]
\})DB"));
  EXPECT_THAT(db, DataBagEqual(db));
  EXPECT_THAT(db, Not(DataBagEqual(DataBagImpl::CreateEmptyDatabag())));
}

TEST(TriplesTest, Attributes) {
  auto ds = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds_a1 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds_a2 = DataSliceImpl::AllocateEmptyObjects(15);
  auto ds_sliced = DataSliceImpl::Create(ds.values<ObjectId>().Slice(0, 14));
  auto ds_a1_sliced =
      DataSliceImpl::Create(ds_a1.values<ObjectId>().Slice(0, 14));

  auto db1 = DataBagImpl::CreateEmptyDatabag();
  auto db2 = DataBagImpl::CreateEmptyDatabag();
  auto db3 = DataBagImpl::CreateEmptyDatabag();
  auto db4 = DataBagImpl::CreateEmptyDatabag();
  auto db5 = DataBagImpl::CreateEmptyDatabag();

  ASSERT_OK(db1->SetAttr(ds, "a", ds_a1));
  ASSERT_OK(db2->SetAttr(ds, "a", ds_a1));                // equal to db1
  ASSERT_OK(db3->SetAttr(ds, "a", ds_a2));                // different values
  ASSERT_OK(db4->SetAttr(ds_sliced, "a", ds_a1_sliced));  // not all values
  ASSERT_OK(db5->SetAttr(ds, "b", ds_a1));  // same values, different attr

  // Matcher (implemented as Triples comparison)
  EXPECT_THAT(db1, DataBagEqual(db2));
  EXPECT_THAT(db1, Not(DataBagEqual(db3)));
  EXPECT_THAT(db1, Not(DataBagEqual(db4)));
  EXPECT_THAT(db1, Not(DataBagEqual(db5)));

  // Forks

  auto db6 = db1->PartiallyPersistentFork();

  EXPECT_THAT(db1, DataBagEqual(db6));

  ASSERT_OK(db6->SetAttr(ds_sliced, "a", ds_a1_sliced));

  EXPECT_THAT(db1, DataBagEqual(db6));

  ASSERT_OK(db6->SetAttr(ds, "a", ds_a2));

  EXPECT_THAT(db1, Not(DataBagEqual(db6)));
  EXPECT_THAT(db3, DataBagEqual(db6));

  auto db7 = db4->PartiallyPersistentFork();

  EXPECT_THAT(db1, Not(DataBagEqual(db7)));

  ASSERT_OK(db7->SetAttr(ds[14], "a", ds_a1[14]));

  EXPECT_THAT(db1, DataBagEqual(db7));

  auto db8 = db5->PartiallyPersistentFork();

  ASSERT_OK(db8->SetAttr(ds, "a", ds_a1));

  EXPECT_THAT(db1, Not(DataBagEqual(db8)));

  ASSERT_OK(
      db8->SetAttr(ds, "b", DataSliceImpl::CreateEmptyAndUnknownType(15)));

  EXPECT_THAT(db1, DataBagEqual(db8));
}

TEST(TriplesTest, DictAndListWithParent) {
  AllocationId dicts = AllocateDicts(2);
  AllocationId lists = AllocateLists(2);
  DataItem dict0(dicts.ObjectByOffset(0));
  DataItem dict1(dicts.ObjectByOffset(1));
  DataItem list0(lists.ObjectByOffset(0));
  DataItem list1(lists.ObjectByOffset(1));

  auto db_a = DataBagImpl::CreateEmptyDatabag();
  auto db_b = DataBagImpl::CreateEmptyDatabag();

  ASSERT_OK(db_a->SetInDict(dict0, DataItem(1), DataItem(2)));
  ASSERT_OK(db_b->SetInDict(dict1, DataItem(3), DataItem(4)));

  ASSERT_OK(db_a->AppendToList(list1, DataItem(5)));
  ASSERT_OK(db_a->AppendToList(list1, DataItem(6)));
  ASSERT_OK(db_b->AppendToList(list1, DataItem(7)));
  ASSERT_OK(db_b->AppendToList(list0, DataItem(8)));

  auto db_a2 = db_a->PartiallyPersistentFork();
  auto db_b2 = db_b->PartiallyPersistentFork();

  EXPECT_THAT(db_a, Not(DataBagEqual(db_b)));
  EXPECT_THAT(db_a, DataBagEqual(db_a2));
  EXPECT_THAT(db_b, DataBagEqual(db_b2));

  ASSERT_OK(db_b2->SetInDict(dict0, DataItem(1), DataItem(2)));
  ASSERT_OK(db_a2->SetInDict(dict1, DataItem(3), DataItem(4)));

  ASSERT_OK(db_a2->AppendToList(list0, DataItem(8)));
  ASSERT_OK(db_a2->RemoveInList(list1, DataBagImpl::ListRange()));
  ASSERT_OK(db_a2->AppendToList(list1, DataItem(7)));

  EXPECT_THAT(db_a2, DataBagEqual(db_b2));
}

}  // namespace
}  // namespace koladata::internal::debug
