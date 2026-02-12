# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
from typing import cast
from unittest import mock

from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import bare_root_initial_data_manager
from koladata.ext.persisted_data import composite_initial_data_manager
from koladata.ext.persisted_data import data_slice_manager_view as data_slice_manager_view_lib
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_util
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager as pidsm
from koladata.ext.persisted_data import stubs_and_minimal_bags_lib


PersistedIncrementalDataSliceManager = (
    pidsm.PersistedIncrementalDataSliceManager
)
DataSliceManagerView = data_slice_manager_view_lib.DataSliceManagerView
BareRootInitialDataManager = (
    bare_root_initial_data_manager.BareRootInitialDataManager
)
CompositeInitialDataManager = (
    composite_initial_data_manager.CompositeInitialDataManager
)
parse_dsp = data_slice_path_lib.DataSlicePath.parse_from_string


class CompositeInitialDataManagerTest(absltest.TestCase):

  def create_manager(
      self, ds: kd.types.DataItem
  ) -> PersistedIncrementalDataSliceManager:
    """Creates a PersistedIncrementalDataSliceManager with the given data."""
    root_item = ds.with_bag(
        stubs_and_minimal_bags_lib.schema_stub(ds).get_bag()
    )
    manager = PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path,
        initial_data_manager=BareRootInitialDataManager.create_new(root_item),
    )
    root = DataSliceManagerView(manager)
    for attr_name in kd.dir(ds):
      setattr(root, attr_name, ds.get_attr(attr_name))
    manager.clear_cache()
    return manager

  def test_simple_glueing(self):
    ds1 = kd.new(
        u=kd.uu(seed='fixed_seed').with_attrs(d=1),
        x=kd.list([1, 2, 3]),
        y='Hello World!',
        z=kd.list([kd.new(a=1, schema='foo'), kd.new(a=2, schema='foo')]),
    )

    ds2 = kd.new(
        v=kd.uu(seed='fixed_seed').with_attrs(d=2),
        x='foo bar baz',
        y=2,
        z=kd.list([kd.new(a=3, schema='foo'), kd.new(a=4, schema='foo')]),
    )

    glue_ds = kd.new(left=ds1.no_bag(), right=ds2.no_bag())

    manager1 = self.create_manager(ds1)
    manager2 = self.create_manager(ds2)
    glue_manager = self.create_manager(glue_ds)
    composite_manager = PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path,
        initial_data_manager=CompositeInitialDataManager.create_new(
            managers=[glue_manager, manager1, manager2]
        ),
        description='Use glue_manager to glue manager1 and manager2 together',
    )
    root = DataSliceManagerView(composite_manager)

    # If we load all the data, we get the same thing as what we would get from
    # standard Koda enrichment.
    full_ds = root.get(populate_including_descendants={root})
    kd.testing.assert_equivalent(
        full_ds,
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()),
        ids_equality=True,
    )

    # We can access data from the composite in the usual way with views.
    kd.testing.assert_equivalent(root.left.z[:].a.get(), kd.slice([1, 2]))
    kd.testing.assert_equivalent(root.right.z[:].a.get(), kd.slice([3, 4]))

    # Data indexing and retrieval is based on the schema. If we ask for a root
    # in which root.left.z is populated, we will also get root.right.z
    # populated, because they use exactly the same struct schema. (This is not a
    # problem, because all data returned to the user is up-to-date and valid.
    # The reason for including all the data is that the indexing and retrieval
    # are by design not aware of the exact aliasing structure inside the data,
    # so it makes pessimistic assumptions and includes everything that could
    # potentially be aliased.)
    kd.testing.assert_equivalent(
        root.get(populate_including_descendants={root.left.z}),
        glue_ds.enriched(
            kd.attrs(glue_ds.left, z=ds1.z), kd.attrs(glue_ds.right, z=ds2.z)
        ),
        ids_equality=True,
    )

    # When itemids are the same across the component managers, the data of the
    # earlier managers in the list take precedence. Aliasing is taken care of.
    kd.testing.assert_equivalent(root.left.u.d.get(), kd.item(1))
    kd.testing.assert_equivalent(root.right.v.d.get(), kd.item(1))

    # The composite manager is isolated from updates to the component managers.
    manager1_root = DataSliceManagerView(manager1)
    kd.testing.assert_equivalent(
        manager1_root.get(populate_including_descendants={manager1_root}),
        ds1,
        ids_equality=True,
    )
    # Update the manager1 by adding a new attribute f to its root:
    manager1_root.f = kd.item(1)
    with self.assertRaises(AssertionError):
      kd.testing.assert_equivalent(
          manager1_root.get(populate_including_descendants={manager1_root}),
          ds1,
          ids_equality=True,
      )
    # The composite manager is not affected by the update to manager1:
    kd.testing.assert_equivalent(
        root.get(populate_including_descendants={root}),
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()),
        ids_equality=True,
    )

    # The isolation also holds for copies of the composite initialized from
    # scratch from disk.
    kd.testing.assert_equivalent(
        PersistedIncrementalDataSliceManager.create_from_dir(
            composite_manager.get_persistence_directory(),
        ).get_data_slice(populate_including_descendants={parse_dsp('')}),
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()),
        ids_equality=True,
    )

    # The composite manager can be updated as usual.
    root.g = kd.item(77), 'Set scalar attribute g to 77'
    kd.testing.assert_equivalent(
        root.get(populate_including_descendants={root}),
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()).updated(
            kd.attrs(glue_ds, g=77)
        ),
        ids_equality=True,
    )
    # This update does not affect any of the component managers:
    for manager, expected_ds in [
        (manager1, ds1.with_attrs(f=1)),
        (manager2, ds2),
        (glue_manager, glue_ds),
    ]:
      kd.testing.assert_equivalent(
          manager.get_data_slice(
              populate_including_descendants={parse_dsp('')}
          ),
          expected_ds,
          ids_equality=True,
      )

    # The composite manager has revisions like usual:
    self.assertEqual(
        [r.description for r in composite_manager.get_revision_history()],
        [
            'Use glue_manager to glue manager1 and manager2 together',
            'Set scalar attribute g to 77',
        ],
    )

    # It persisted the updates immediately, as usual. New instances initialized
    # from disk will have the same data and metadata.
    kd.testing.assert_equivalent(
        PersistedIncrementalDataSliceManager.create_from_dir(
            composite_manager.get_persistence_directory(),
        ).get_data_slice(populate_including_descendants={parse_dsp('')}),
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()).updated(
            kd.attrs(glue_ds, g=77)
        ),
        ids_equality=True,
    )
    self.assertEqual(
        PersistedIncrementalDataSliceManager.create_from_dir(
            composite_manager.get_persistence_directory(),
        ).get_revision_history(),
        composite_manager.get_revision_history(),
    )

    # The composite manager can be branched as usual.
    branch_composite_manager = composite_manager.branch(
        self.create_tempdir().full_path,
        revision_history_index=0,  # Roll back the setting of g.
        description='A branch of the composite manager',
    )
    branch_root = DataSliceManagerView(branch_composite_manager)
    kd.testing.assert_equivalent(
        branch_root.get(populate_including_descendants={branch_root}),
        glue_ds.enriched(ds1.get_bag(), ds2.get_bag()),
        ids_equality=True,
    )

    # It can also be filtered as usual. Here, we filter inside the branch.
    branch_left_z_a = branch_root.left.z[:].a
    kd.testing.assert_equivalent(branch_left_z_a.get(), kd.slice([1, 2]))
    branch_left_z_a.filter(branch_left_z_a.get() % 2 == 0)
    kd.testing.assert_equivalent(branch_left_z_a.get(), kd.slice([2]))

    # The filtering does not affect the trunk - branches are isolated as usual.
    kd.testing.assert_equivalent(root.left.z[:].a.get(), kd.slice([1, 2]))

  def test_join_operations(self):
    query1_schema = kd.schema.new_schema()
    doc1_schema = kd.schema.new_schema()
    new_query1 = query1_schema.new
    new_doc1 = doc1_schema.new
    ds1 = kd.new(
        query=kd.list([
            new_query1(
                id=1,
                text='text1',
                doc=new_doc1(
                    id=kd.slice([10, 11, 12, 13]),
                    title=kd.slice(['d10', 'd11', 'd12', 'd13']),
                ).implode(),
            ),
            new_query1(
                id=2,
                text='text2',
                doc=new_doc1(
                    id=kd.slice([20, 21, 22, 23]),
                    title=kd.slice(['d20', 'd21', 'd22', 'd23']),
                ).implode(),
            ),
        ])
    )
    manager1 = self.create_manager(ds1)
    root1 = DataSliceManagerView(manager1)

    # To illustrate that we can join DataSlices with different schemas, we use
    # different schemas for the queries and docs here.
    query2_schema = kd.schema.new_schema()
    doc2_schema = kd.schema.new_schema()
    new_query2 = query2_schema.new
    new_doc2 = doc2_schema.new
    ds2 = kd.new(
        query=kd.list([
            new_query2(
                id=3,
                text='text2',
                doc=new_doc2(
                    id=kd.slice([30, 32, 31, 20, 11]),
                    x=kd.slice([130, 132, 131, 120, 111]),
                ).implode(),
            ),
            new_query2(
                id=4,
                text='text4',
                doc=new_doc2(
                    id=kd.slice([20, 21, 32, 12]),
                    x=kd.slice([120, 121, 132, 112]),
                ).implode(),
            ),
            new_query2(
                id=5,
                text='text2',
                doc=new_doc2(
                    id=kd.slice([30, 20, 23]),
                    x=kd.slice([130, 120, 123]),
                ).implode(),
            ),
        ])
    )
    manager2 = self.create_manager(ds2)
    root2 = DataSliceManagerView(manager2)
    # As usual, we can create reference cycles in the data. For example, we can
    # broadcast the query itemid into the doc and save it there in an attribute.
    # That will come in handy later, because we will join docs, and then we will
    # be able to access the joined query from the joined doc.
    root2.query[:].doc[:].query = root2.query[:].get()

    # Suppose we want to join manager1 and manager2 using a left join on the
    # composite key (query.text, doc.id). This is how we could proceed:
    #
    # Step 1. Get minimal DataSlices from the two managers with only the data
    # needed for the join. They're called "skeletons" in the code. They are
    # ordinary non-incremental Koda DataSlices.
    skeleton1 = root1.get(
        populate={root1.query[:].text, root1.query[:].doc[:].id}
    )
    skeleton1 = cast(kd.types.DataItem, skeleton1)  # please pytype
    skeleton2 = root2.get(
        populate={root2.query[:].text, root2.query[:].doc[:].id}
    )
    skeleton2 = cast(kd.types.DataItem, skeleton2)  # please pytype
    #
    # Step 2. Join the skeletons using standard Koda operations. The result is
    # a "join skeleton" - an ordinary non-incremental Koda DataSlice.
    left_join_skeleton = skeleton1.updated(
        kd.attrs(
            skeleton1.query[:].doc[:],
            joined_docs=kd.translate_group(
                keys_to=kd.uuid(
                    key_part1=skeleton1.query[:].text,
                    key_part2=skeleton1.query[:].doc[:].id,
                ),
                keys_from=kd.uuid(
                    key_part1=skeleton2.query[:].text,
                    key_part2=skeleton2.query[:].doc[:].id,
                ).flatten(),
                values_from=skeleton2.query[:].doc[:].no_bag().flatten(),
            ).implode(),
        )
    )
    #
    # Step 3. Create a new PersistedIncrementalDataSliceManager from the join
    # skeleton.
    left_join_skeleton_manager = self.create_manager(left_join_skeleton)
    #
    # Step 4. Create a composite manager that glues everything together.
    left_join_manager = PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path,
        initial_data_manager=CompositeInitialDataManager.create_new(
            managers=[left_join_skeleton_manager, manager1, manager2]
        ),
        description='Left join manager1 and manager2 on (query.text, doc.id)',
    )
    root = DataSliceManagerView(left_join_manager)

    # That's it!
    # The composite manager knows everything about the join. It understands how
    # to attach the full data to the join skeleton so that users can access the
    # full data of the join incrementally and on demand.
    kd.testing.assert_equivalent(
        root.query[:].doc[:].joined_docs[:].x.get(),
        kd.slice([[[], [], [], []], [[120, 120], [], [], [123]]]),
    )
    kd.testing.assert_equivalent(
        root.query[:].doc[:].joined_docs[:].query.id.get(),
        kd.slice([[[], [], [], []], [[3, 5], [], [], [5]]]),
    )

    # We saw the left join above. It's easy to use filtering to obtain the inner
    # join. Let's do that in a branch:
    manager_branch = left_join_manager.branch(self.create_tempdir().full_path)
    branch_root = DataSliceManagerView(manager_branch)
    branch_root.query[:].doc[:].filter(
        # Note that we do `joined_docs[:].get().implode()` here and not
        # `joined_docs.get()`. The reason is that `joined_docs.get()` returns
        # only the list itemids, and they don't have any information about the
        # sizes of the lists.
        kd.lists.size(
            branch_root.query[:].doc[:].joined_docs[:].get().implode()
        )
        > 0
    )
    # Now it represents the inner join, from the perspective of the left side:
    kd.testing.assert_equivalent(
        branch_root.query[:].doc[:].joined_docs[:].x.get(),
        kd.slice([[[120, 120], [123]]]),
    )
    kd.testing.assert_equivalent(
        branch_root.query[:].doc[:].joined_docs[:].query.id.get(),
        kd.slice([[[3, 5], [5]]]),
    )
    kd.testing.assert_equivalent(
        branch_root.query[:].id.get(),
        kd.slice([2]),
    )
    kd.testing.assert_equivalent(
        branch_root.query[:].doc[:].id.get(),
        kd.slice([[20, 23]]),
    )
    left_join_row_schema = kd.schema.new_schema()
    kd.testing.assert_equivalent(
        left_join_row_schema.new(
            left_query_id=branch_root.query[:].id.get(),
            doc_id=branch_root.query[:].doc[:].id.get(),
            right_query_id=(
                branch_root.query[:].doc[:].joined_docs[:].query.id.get()
            ),
        ).flatten(),
        kd.slice([
            left_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=3,
            ),
            left_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=5,
            ),
            left_join_row_schema.new(
                left_query_id=2,
                doc_id=23,
                right_query_id=5,
            ),
        ]),
    )

    # A right join is analogous to a left join.
    # Again, we can store references to the queries in the docs.
    root1.query[:].doc[:].query = root1.query[:].get()
    # We want to join manager1 and manager2 using a right join on the composite
    # key (query.text, doc.id). The steps from above now look like this:
    #
    # Step 1. The skeletons are the same those from the left join above.
    #
    # Step 2. Join the skeletons using standard Koda operations. The result is
    # a "join skeleton" - an ordinary non-incremental Koda DataSlice. Notice
    # that the operation here is the same as for the left join but it switches
    # the roles of skeleton1 and skeleton2.
    right_join_skeleton = skeleton2.updated(
        kd.attrs(
            skeleton2.query[:].doc[:],
            joined_docs=kd.translate_group(
                keys_to=kd.uuid(
                    key_part1=skeleton2.query[:].text,
                    key_part2=skeleton2.query[:].doc[:].id,
                ),
                keys_from=kd.uuid(
                    key_part1=skeleton1.query[:].text,
                    key_part2=skeleton1.query[:].doc[:].id,
                ).flatten(),
                values_from=skeleton1.query[:].doc[:].no_bag().flatten(),
            ).implode(),
        )
    )
    #
    # Step 3. Create a new PersistedIncrementalDataSliceManager from the join
    # skeleton.
    right_join_skeleton_manager = self.create_manager(right_join_skeleton)
    #
    # Step 4. Create a composite manager that glues everything together.
    right_join_manager = PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path,
        initial_data_manager=CompositeInitialDataManager.create_new(
            managers=[right_join_skeleton_manager, manager1, manager2]
        ),
        description='Right join manager1 and manager2 on (query.text, doc.id)',
    )
    root = DataSliceManagerView(right_join_manager)
    # That's it!
    # The composite manager knows everything about the join. It understands how
    # to attach the full data to the join skeleton so that users can access the
    # full data of the join incrementally and on demand.
    kd.testing.assert_equivalent(
        root.query[:].doc[:].joined_docs[:].title.get(),
        kd.slice([
            [[], [], [], ['d20'], []],
            [[], [], [], []],
            [[], ['d20'], ['d23']],
        ]),
    )
    kd.testing.assert_equivalent(
        root.query[:].doc[:].joined_docs[:].query.id.get(),
        kd.slice([[[], [], [], [2], []], [[], [], [], []], [[], [2], [2]]]),
    )

    # We can again user filtering to obtain the inner join, this time from the
    # perspective of the right side.
    manager_branch = right_join_manager.branch(self.create_tempdir().full_path)
    branch_root = DataSliceManagerView(manager_branch)
    branch_root.query[:].doc[:].filter(
        kd.lists.size(
            branch_root.query[:].doc[:].joined_docs[:].get().implode()
        )
        > 0
    )
    # Now it represents the inner join, from the perspective of the right side:
    kd.testing.assert_equivalent(
        branch_root.query[:].doc[:].x.get(),
        kd.slice([[120], [120, 123]]),
    )
    right_join_row_schema = kd.schema.new_schema()
    kd.testing.assert_equivalent(
        right_join_row_schema.new(
            left_query_id=(
                branch_root.query[:].doc[:].joined_docs[:].query.id.get()
            ),
            doc_id=branch_root.query[:].doc[:].id.get(),
            right_query_id=branch_root.query[:].id.get(),
        ).flatten(),
        kd.slice([
            right_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=3,
            ),
            right_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=5,
            ),
            right_join_row_schema.new(
                left_query_id=2,
                doc_id=23,
                right_query_id=5,
            ),
        ]),
    )

    # It is also easy to have an inner join that is not from the perspective of
    # any particular side. Here is an example:
    inner_join_skeleton = kd.new(
        doc_pair=kd.new(
            left=skeleton1.query[:].doc[:],
            right=kd.translate_group(
                keys_to=kd.uuid(
                    key_part1=skeleton1.query[:].text,
                    key_part2=skeleton1.query[:].doc[:].id,
                ),
                keys_from=kd.uuid(
                    key_part1=skeleton2.query[:].text,
                    key_part2=skeleton2.query[:].doc[:].id,
                ).flatten(),
                values_from=skeleton2.query[:].doc[:].no_bag().flatten(),
            ),
        )
        .select(lambda doc_pair: kd.has(doc_pair.left) & kd.has(doc_pair.right))
        .flatten()
        .implode()
    )
    inner_join_skeleton_manager = self.create_manager(inner_join_skeleton)
    inner_join_manager = PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path,
        initial_data_manager=CompositeInitialDataManager.create_new(
            managers=[inner_join_skeleton_manager, manager1, manager2]
        ),
        description='Inner join manager1 and manager2 on (query.text, doc.id)',
    )
    inner_join_root = DataSliceManagerView(inner_join_manager)
    inner_join_doc_pair = inner_join_root.doc_pair[:]
    kd.testing.assert_equivalent(
        # The doc id was part of the join key, so it must be the same on both
        # sides.
        inner_join_doc_pair.left.id.get(),
        inner_join_doc_pair.right.id.get(),
    )
    inner_join_row_schema = kd.schema.new_schema()
    kd.testing.assert_equivalent(
        inner_join_row_schema.new(
            left_query_id=inner_join_doc_pair.left.query.id.get(),
            doc_id=inner_join_doc_pair.left.id.get(),
            right_query_id=inner_join_doc_pair.right.query.id.get(),
        ),
        kd.slice([
            inner_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=3,
            ),
            inner_join_row_schema.new(
                left_query_id=2,
                doc_id=20,
                right_query_id=5,
            ),
            inner_join_row_schema.new(
                left_query_id=2,
                doc_id=23,
                right_query_id=5,
            ),
        ]),
    )

  def test_composition_needs_at_least_one_manager(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('at least one manager is required'),
    ):
      CompositeInitialDataManager.create_new(managers=[])

  def test_calling_constructor_directly_raises(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'please do not call the CompositeInitialDataManager constructor'
            ' directly; use the class factory methods create_new() or'
            ' deserialize() instead'
        ),
    ):
      CompositeInitialDataManager(
          internal_call=object(), managers=[self.create_manager(kd.new())]
      )

  def test_serialization_to_non_empty_dir_raises(self):
    manager = CompositeInitialDataManager.create_new(
        managers=[self.create_manager(kd.new())]
    )

    persistence_dir = self.create_tempdir().full_path
    with open(os.path.join(persistence_dir, 'some_file'), 'w') as f:
      f.write('some content')

    with self.assertRaisesRegex(
        ValueError,
        re.escape(f'the given persistence_dir {persistence_dir} is not empty'),
    ):
      manager.serialize(
          persistence_dir, fs=fs_util.get_default_file_system_interaction()
      )

  def test_get_data_bag_for_invalid_schema_node_names_raises(self):
    manager = CompositeInitialDataManager.create_new(
        managers=[self.create_manager(kd.new())]
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "schema_node_names contains invalid entries: frozenset({'hohoho!'})"
        ),
    ):
      manager.get_data_bag_for_schema_node_names(['hohoho!'])

  def test_clear_cache(self):
    manager1 = self.create_manager(kd.new())
    manager2 = self.create_manager(kd.new())

    manager = CompositeInitialDataManager.create_new(
        managers=[manager1, manager2]
    )
    manager_mocks = [mock.Mock(wraps=m) for m in manager._managers]
    manager._managers = manager_mocks
    manager.clear_cache()

    self.assertLen(manager_mocks, 2)
    manager_mocks[0].clear_cache.assert_called_once()
    manager_mocks[1].clear_cache.assert_called_once()

  def test_get_description(self):
    manager1 = self.create_manager(kd.new())
    manager2 = self.create_manager(kd.new())
    manager = CompositeInitialDataManager.create_new(
        managers=[manager1, manager2]
    )

    self.assertEqual(
        manager.get_description(),
        'a composition of the managers'
        f' [{manager1.get_persistence_directory()},'
        f' {manager2.get_persistence_directory()}]',
    )

  def test_internal_managers_are_read_only(self):
    manager1 = self.create_manager(kd.new())
    manager2 = self.create_manager(kd.new())
    manager = CompositeInitialDataManager.create_new(
        managers=[manager1, manager2]
    )
    self.assertTrue(manager._managers[0].is_read_only)
    self.assertTrue(manager._managers[1].is_read_only)


if __name__ == '__main__':
  absltest.main()
