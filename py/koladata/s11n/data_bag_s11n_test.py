# Copyright 2024 Google LLC
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

"""Tests for data_slice_s11n."""

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
    db = kd.bag()
    self.assertDumpsEqual(db.qtype, text)
    self.assertLoadsEqual(text, db.qtype)

  def test_empty_bag(self):
    db = kd.bag()
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_single_object(self):
    db = kd.bag()
    db.obj(a=1, b='2', c=kd.list([3, 4]), d=kd.dict({'a': 'b', 'c': 'd'}))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_objects(self):
    db = kd.bag()
    l1 = db.list(['a, b'])
    l2 = db.list(['c, d'])
    db.obj(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([l1, l2]))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_entities(self):
    db = kd.bag()
    d1 = db.dict({'a': 'b'})
    d2 = db.dict({'c': 'd'})
    db.new(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([d1, d2]))
    data = arolla.s11n.dumps(db)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, db)

  def test_complicated_case_with_fallbacks(self):
    db1 = kd.bag()
    l1 = db1.list(['a, b'])
    l2 = db1.list(['c, d'])
    obj = db1.new(a=kd.slice([1, 2]), b=kd.slice([3, 4]), c=kd.slice([l1, l2]))

    db2 = kd.bag()
    obj.with_db(db2).set_attr(
        'd', kd.dict({'a': 'b', 'c': kd.obj(l1)}), update_schema=True
    )

    slice_with_fallback = db2.new(a=5, b=7).with_fallback(db1)

    data = arolla.s11n.dumps_many([db1, db2, slice_with_fallback], [])
    values, _ = arolla.s11n.loads_many(data)
    self.assertLen(values, 3)
    kd.testing.assert_equivalent(values[0], db1)
    kd.testing.assert_equivalent(values[1], db2)
    kd.testing.assert_equivalent(values[2], slice_with_fallback)

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