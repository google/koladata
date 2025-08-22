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

import re

from absl.testing import absltest
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from koladata import kd

from koladata.s11n import codec_pb2 as _


class DataBagS11NTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            data_bag_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    db = kd.mutable_bag()
    self.assertDumpsEqual(db.qtype, text)
    self.assertLoadsEqual(text, db.qtype)

  def test_empty_bag(self):
    db = kd.mutable_bag()
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_single_object(self):
    db = kd.mutable_bag()
    db.obj(a=1, b='2', c=kd.list([3, 4]), d=kd.dict({'a': 'b', 'c': 'd'}))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_objects(self):
    db = kd.mutable_bag()
    l1 = db.list(['a, b'])
    l2 = db.list(['c, d'])
    db.obj(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([l1, l2]))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_entities(self):
    db = kd.mutable_bag()
    d1 = db.dict({'a': 'b'})
    d2 = db.dict({'c': 'd'})
    db.new(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([d1, d2]))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_complicated_case_with_fallbacks(self):
    db1 = kd.mutable_bag()
    l1 = db1.list(['a, b'])
    l2 = db1.list(['c, d'])
    obj = db1.new(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([l1, l2]))

    db2 = kd.mutable_bag()
    obj.with_bag(db2).set_attr('d', kd.dict({'a': 'b', 'c': kd.obj(l1)}))

    slice_with_fallback = db2.new(a=5, b=7).enriched(db1)

    data = arolla.s11n.dumps_many([db1, db2, slice_with_fallback], [])
    values, _ = arolla.s11n.loads_many(data)
    self.assertLen(values, 3)
    kd.testing.assert_equivalent(values[0], db1)
    kd.testing.assert_equivalent(values[1], db2)
    kd.testing.assert_equivalent(values[2], slice_with_fallback)

  def test_removed_values(self):
    db = kd.mutable_bag()
    obj_single = db.new(a=1, b=None, c=2)
    obj_dense = db.new(x=kd.slice([1, 2, None, 4, 5, 7, 8, 9, 10] * 10))
    (obj_dense & (obj_dense.x > 2)).y = 17
    (obj_dense & (obj_dense.x > 4)).y = None
    (obj_dense & (obj_dense.x > 8)).z = 37
    (obj_dense & (obj_dense.x > 9)).z = None

    fb = kd.mutable_bag()
    fb_obj_single = obj_single.with_bag(fb)
    fb_obj_single.a = 77
    fb_obj_single.b = 97
    fb_obj_single.d = 57

    fb_obj_dense = obj_dense.with_bag(fb)
    fb_obj_dense.x = kd.slice([-7] * 90)
    fb_obj_dense.y = kd.slice([-39, None, -57, -4, -5, -7, None, -9, None] * 10)
    fb_obj_dense.z = kd.slice(
        [-39, None, -57, -43, None, -17, None, -9, -77] * 10
    )

    obj_single_with_fb = obj_single.enriched(fb)
    obj_dense_with_fb = obj_dense.enriched(fb)

    data = arolla.s11n.dumps_many(
        [db, fb, obj_single_with_fb, obj_dense_with_fb], []
    )
    values, _ = arolla.s11n.loads_many(data)
    self.assertLen(values, 4)
    kd.testing.assert_equivalent(values[0], db)
    kd.testing.assert_equivalent(values[1], fb)
    kd.testing.assert_equivalent(values[2], obj_single_with_fb)
    for attr in ['a', 'c', 'd']:
      with self.subTest(f'single_object_{attr}'):
        kd.testing.assert_equivalent(
            getattr(values[2], attr),
            getattr(obj_single_with_fb, attr))
    kd.testing.assert_equivalent(values[3], obj_dense_with_fb)
    for attr in ['x', 'y', 'z']:
      with self.subTest(f'dense_object_{attr}'):
        kd.testing.assert_equivalent(
            getattr(values[3], attr),
            getattr(obj_dense_with_fb, attr))

  def test_dict_removed_values(self):
    db = kd.mutable_bag()
    d = db.dict()
    fb = db.fork()

    d['a'] = 1
    d['b'] = None
    self.assertIsNone(d['b'].to_py())
    d['c'] = 3
    del d['c']

    fb_d = d.with_bag(fb)
    fb_d['a'] = 77
    fb_d['b'] = 97
    fb_d['d'] = 57

    d = d.enriched(fb)

    data = kd.dumps(d)
    actual_d = kd.loads(data)

    self.assertEqual(actual_d['a'].to_py(), 1)
    self.assertIsNone(actual_d['b'].to_py())
    self.assertIsNone(actual_d['c'].to_py())
    self.assertEqual(actual_d['d'].to_py(), 57)

  def test_removed_lists(self):
    lists = kd.implode(kd.slice([[1, 2], [3], [4, 5]]))

    l1, l2, l3 = lists.S[0], lists.S[1], lists.S[2]

    db = kd.mutable_bag()
    fb = kd.mutable_bag()

    db.adopt(l1.get_schema())
    fb.adopt(l1.get_schema())

    # lists in db: [[1], [], None]
    l1.with_bag(db).append(1)
    l2.with_bag(db).append(2)
    l2.with_bag(db).pop(0)

    # lists in fb: [[3], [4], [5]]
    l1.with_bag(fb).append(3)
    l2.with_bag(fb).append(4)
    l3.with_bag(fb).append(5)

    self.assertEqual(lists.with_bag(db).enriched(fb).to_py(), [[1], [], [5]])

    data = kd.dumps(db)
    db2 = kd.loads(data)

    self.assertEqual(lists.with_bag(db2).enriched(fb).to_py(), [[1], [], [5]])

  def test_obj_with_databag(self):
    v = kd.obj(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6]))
    data = arolla.s11n.dumps(v)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, v)

  def test_decoder_errors(self):
    header_and_deps = """
      version: 1
      codecs {
        name: "koladata.s11n"
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            internal_data_item_value {
              object_id {
                hi: 16432670451961878383
                lo: 10532836424664956994
              }
            }
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            internal_data_item_value {
              i32: 1
            }
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            internal_data_item_value {
              text: "2"
            }
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            data_slice_impl_value {
              data_item_vector {
                values {
                  i32: 1
                }
                values {
                  i32: 2
                }
              }
            }
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError, re.escape('invalid input value index: 1')
    ):
      self.parse_container_text_proto(header_and_deps + """
          decoding_steps {
            value {
              input_value_indices: 0
              codec_index: 0
              [koladata.s11n.KodaV1Proto.extension] {
                data_bag_value {
                  fallback_count: 0
                  attrs {
                    name: "a"
                    chunks {
                      first_object_id {
                        hi: 2366103575210409
                        lo: 13
                      }
                      values_subindex: 1
                    }
                  }
                }
              }
            }
          }
          output_value_indices: 4
          """)
    with self.assertRaisesRegex(
        ValueError, re.escape('only empty DataBag can have fallbacks')
    ):
      self.parse_container_text_proto(header_and_deps + """
          decoding_steps {
            value {
              input_value_indices: 0
              input_value_indices: 1
              codec_index: 0
              [koladata.s11n.KodaV1Proto.extension] {
                data_bag_value {
                  fallback_count: 1
                  attrs {
                    name: "a"
                    chunks {
                      first_object_id {
                        hi: 2366103575210409
                        lo: 13
                      }
                      values_subindex: 1
                    }
                  }
                }
              }
            }
          }
          output_value_indices: 4
          """)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "AttrChunkProto values don't fit into AllocationId of"
            ' `first_object_id`: values size is 2, alloc capacity is 1'
        ),
    ):
      self.parse_container_text_proto(header_and_deps + """
          decoding_steps {
            value {
              input_value_indices: 3
              codec_index: 0
              [koladata.s11n.KodaV1Proto.extension] {
                data_bag_value {
                  fallback_count: 0
                  attrs {
                    name: "a"
                    chunks {
                      first_object_id {
                        hi: 2366103575210409
                        lo: 13
                      }
                      values_subindex: 0
                    }
                  }
                }
              }
            }
          }
          output_value_indices: 4
          """)


if __name__ == '__main__':
  absltest.main()
