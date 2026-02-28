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
#include "koladata/internal/op_utils/json_stream.h"

#include <string>

#include "gtest/gtest.h"
#include "koladata/internal/op_utils/stream_processor_state.pb.h"

namespace koladata::internal {
namespace {

constexpr JsonSalvageOptions kDefaultJsonSalvageOptions = {
    .allow_nan = false,
    .ensure_ascii = false,
    .max_depth = 100,
};

constexpr JsonSalvageOptions kEnsureAsciiJsonSalvageOptions = {
    .allow_nan = false,
    .ensure_ascii = true,
    .max_depth = 100,
};

constexpr JsonSalvageOptions kAllowNanJsonSalvageOptions = {
    .allow_nan = true,
    .ensure_ascii = false,
    .max_depth = 100,
};

std::string RunSalvage(std::string input, const JsonSalvageOptions& options =
                                              kDefaultJsonSalvageOptions) {
  JsonSalvageStreamProcessor processor(options);
  std::string output;
  output += processor.ProcessInputChunk(input);
  output += processor.ProcessEnd();

  // Also run the processor on a single byte of input at a time, serializing
  // and deserializing in between each byte. This should produce the same
  // concatenated result.
  std::string byte_streamed_output;
  processor.Reset();
  std::string state = processor.ToState();
  for (char c : input) {
    std::string input_chunk;
    input_chunk.push_back(c);
    {
      JsonSalvageStreamProcessor tmp_processor(options);
      EXPECT_TRUE(tmp_processor.LoadState(state));
      byte_streamed_output += tmp_processor.ProcessInputChunk(input_chunk);
      state = tmp_processor.ToState();
    }
  }
  {
    JsonSalvageStreamProcessor tmp_processor(options);
    EXPECT_TRUE(tmp_processor.LoadState(state));
    byte_streamed_output += tmp_processor.ProcessEnd();
  }

  EXPECT_EQ(byte_streamed_output, output);

  return output;
}

TEST(JsonStreamTest, SalvageStreamProcessorValidJson) {
  EXPECT_EQ(RunSalvage(""), "");
  EXPECT_EQ(RunSalvage("null"), "null");
  EXPECT_EQ(RunSalvage("true"), "true");
  EXPECT_EQ(RunSalvage("false"), "false");
  EXPECT_EQ(RunSalvage("0"), "0");
  EXPECT_EQ(RunSalvage("1"), "1");
  EXPECT_EQ(RunSalvage("-1"), "-1");
  EXPECT_EQ(RunSalvage("0.0"), "0.0");
  EXPECT_EQ(RunSalvage("0.5"), "0.5");
  EXPECT_EQ(RunSalvage("1.5"), "1.5");
  EXPECT_EQ(RunSalvage("-0.5"), "-0.5");
  EXPECT_EQ(RunSalvage("-1.5"), "-1.5");
  EXPECT_EQ(RunSalvage("0.0e0"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.0e+0"), "0.0e+0");
  EXPECT_EQ(RunSalvage("0.0e-0"), "0.0e-0");
  EXPECT_EQ(RunSalvage("0e0"), "0e0");
  EXPECT_EQ(RunSalvage("0e-0"), "0e-0");
  EXPECT_EQ(RunSalvage("0e+0"), "0e+0");
  EXPECT_EQ(RunSalvage("1e100"), "1e100");
  EXPECT_EQ(RunSalvage("1e-100"), "1e-100");
  EXPECT_EQ(RunSalvage("1e+100"), "1e+100");
  EXPECT_EQ(RunSalvage("123.456e789"), "123.456e789");
  EXPECT_EQ(
      RunSalvage(
          "62302303891805340889130936088901407870066683052978661211675247124802"
          "24062798795284344852331075696347236418294203078437869057448630895929"
          "15854407457543913287819999913057019313870887275682423675743947927"),
      "623023038918053408891309360889014078700666830529786612116752471248022406"
      "279879528434485233107569634723641829420307843786905744863089592915854407"
      "457543913287819999913057019313870887275682423675743947927");
  EXPECT_EQ(
      RunSalvage("1030541808740199603499876866000153749572039622019058299928215"
                 "1713682590638133055005292735058819464876."
                 "6429973272012106081676174457299671868094444157442462036306814"
                 "0448515660259518722102551751709055957792e84792320588661664766"
                 "8005906681471239960534669959508339683692278089823445350301719"
                 "01840884058045093129"),
      "103054180874019960349987686600015374957203962201905829992821517136825906"
      "38133055005292735058819464876."
      "642997327201210608167617445729967186809444415744246203630681404485156602"
      "59518722102551751709055957792e847923205886616647668005906681471239960534"
      "66995950833968369227808982344535030171901840884058045093129");
  EXPECT_EQ(RunSalvage("\"\""), "\"\"");
  EXPECT_EQ(RunSalvage(" \"\" "), "\"\"");
  EXPECT_EQ(RunSalvage("\"abc\""), "\"abc\"");
  EXPECT_EQ(RunSalvage("\"*\""), "\"*\"");
  EXPECT_EQ(RunSalvage("\"\\\\\\\"\\b\\f\\n\\r\\t\""),
            "\"\\\\\\\"\\b\\f\\n\\r\\t\"");
  EXPECT_EQ(RunSalvage("\"\\u000a\""), "\"\\n\"");
  EXPECT_EQ(RunSalvage("\"\\u0022\""), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("\"\\u005c\""), "\"\\\\\"");
  EXPECT_EQ(RunSalvage("\" \""), "\" \"");
  EXPECT_EQ(RunSalvage("\"½\""), "\"½\"");
  EXPECT_EQ(RunSalvage("\"\\u00bd\""), "\"½\"");
  EXPECT_EQ(RunSalvage("\"½\"", kEnsureAsciiJsonSalvageOptions), "\"\\u00bd\"");
  EXPECT_EQ(RunSalvage("\"\\u00bd\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\u00bd\"");
  EXPECT_EQ(RunSalvage("\"♠\""), "\"♠\"");
  EXPECT_EQ(RunSalvage("\"\\u2660\""), "\"♠\"");
  EXPECT_EQ(RunSalvage("\"♠\"", kEnsureAsciiJsonSalvageOptions), "\"\\u2660\"");
  EXPECT_EQ(RunSalvage("\"\\u2660\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\u2660\"");
  EXPECT_EQ(RunSalvage("\"😊\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\ud83d\\ude0a\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"😊\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ud83d\\ude0a\"");
  EXPECT_EQ(RunSalvage("\"\\ud83d\\ude0a\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ud83d\\ude0a\"");
  EXPECT_EQ(RunSalvage("[]"), "[]");
  EXPECT_EQ(RunSalvage("[true,false,null]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[true, false, null]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[ true, false, null ]"), "[true,false,null]");
  EXPECT_EQ(RunSalvage("[[],[[],[]]]"), "[[],[[],[]]]");
  EXPECT_EQ(RunSalvage("{}"), "{}");
  EXPECT_EQ(RunSalvage("{\"x\":\"y\"}"), "{\"x\":\"y\"}");
  EXPECT_EQ(RunSalvage("{\"x\":\"y\",\"z\":true}"), "{\"x\":\"y\",\"z\":true}");
  EXPECT_EQ(RunSalvage("{\"x\":{\"y\":\"z\"}}"), "{\"x\":{\"y\":\"z\"}}");

  EXPECT_EQ(
      RunSalvage(R"json(
  {
    "foo": [1, null, "bar", {}],
    "coords": {
      "x": 123.45,
      "y": -111.1e2,
      "z": null
    }
  }
  )json"),
      R"json({"foo":[1,null,"bar",{}],"coords":{"x":123.45,"y":-111.1e2,"z":null}})json");

  EXPECT_EQ(RunSalvage("0 0 0"), "0\n0\n0");
  EXPECT_EQ(RunSalvage("null null null"), "null\nnull\nnull");
  EXPECT_EQ(RunSalvage("[] [[]] [[[]]]"), "[]\n[[]]\n[[[]]]");
}

constexpr auto kRealisticJsonDocument = R"json(
{
  "metadata": {
    "id": "7f8b-4c21-9a03",
    "version": 2.0,
    "active": true,
    "tags": ["production", "api-v2", "nesting-test"],
    "checksum": null
  },
  "deep_nesting": {
    "level_1": {
      "level_2": {
        "level_3": [
          {
            "node_type": "leaf",
            "coordinates": [34.0522, -118.2437],
            "data_stream": [
              [1, 0, 0],
              [0, 1, 0],
              [0, 0, 1]
            ]
          },
          {
            "node_type": "branch",
            "recursive_ref": {
              "depth_limit": 10,
              "overflow": false
            }
          }
        ]
      }
    }
  },
  "data_types_showcase": {
    "strings": {
      "unicode": "Guten Tag, 世界",
      "escaped": "Line one\nLine two with \"quotes\" and a backslash: \\",
      "empty": ""
    },
    "numbers": {
      "integer": 42,
      "negative": -128,
      "floating_point": 3.1415926535,
      "scientific_notation": 6.022e23,
      "small_notation": 1.602e-19
    },
    "booleans_and_null": [true, false, null],
    "mixed_array": [
      100,
      "text",
      {"key": "value"},
      [1, 2, 3],
      null,
      true
    ]
  },
  "system_logs": [
    {
      "timestamp": "2026-02-26T08:38:37Z",
      "severity": "INFO",
      "message": "System check complete",
      "details": {
        "uptime": 86400,
        "load_averages": [0.15, 0.22, 0.05]
      }
    },
    {
      "timestamp": "2026-02-26T08:45:00Z",
      "severity": "WARNING",
      "message": "Memory threshold reached",
      "metrics": {
        "used_mb": 15420,
        "total_mb": 16384,
        "percentage": 94.12
      }
    }
  ],
  "configuration_overrides": {
    "feature_flags": {
      "beta_ui": true,
      "dark_mode": false,
      "experimental_engine": null
    },
    "user_preferences": {
      "theme": "nebula-dark",
      "font_size": 14,
      "notifications": {
        "email": true,
        "push": false,
        "sms": false
      }
    }
  }
}
)json";

// Formatted using python
// json.dumps(x, indent=None, separators=',:', ensure_ascii=False)
// with one extraneous `+` removed from `6.022e+23`.
constexpr auto kRealisticJsonDocumentWithoutWhitespace =
    R"json({"metadata":{"id":"7f8b-4c21-9a03","version":2.0,"active":true,"tags":["production","api-v2","nesting-test"],"checksum":null},"deep_nesting":{"level_1":{"level_2":{"level_3":[{"node_type":"leaf","coordinates":[34.0522,-118.2437],"data_stream":[[1,0,0],[0,1,0],[0,0,1]]},{"node_type":"branch","recursive_ref":{"depth_limit":10,"overflow":false}}]}}},"data_types_showcase":{"strings":{"unicode":"Guten Tag, 世界","escaped":"Line one\nLine two with \"quotes\" and a backslash: \\","empty":""},"numbers":{"integer":42,"negative":-128,"floating_point":3.1415926535,"scientific_notation":6.022e23,"small_notation":1.602e-19},"booleans_and_null":[true,false,null],"mixed_array":[100,"text",{"key":"value"},[1,2,3],null,true]},"system_logs":[{"timestamp":"2026-02-26T08:38:37Z","severity":"INFO","message":"System check complete","details":{"uptime":86400,"load_averages":[0.15,0.22,0.05]}},{"timestamp":"2026-02-26T08:45:00Z","severity":"WARNING","message":"Memory threshold reached","metrics":{"used_mb":15420,"total_mb":16384,"percentage":94.12}}],"configuration_overrides":{"feature_flags":{"beta_ui":true,"dark_mode":false,"experimental_engine":null},"user_preferences":{"theme":"nebula-dark","font_size":14,"notifications":{"email":true,"push":false,"sms":false}}}})json";

TEST(JsonStreamTest, SalvageStreamProcessorRealisticJsonDocument) {
  EXPECT_EQ(RunSalvage(kRealisticJsonDocument),
            kRealisticJsonDocumentWithoutWhitespace);
}

constexpr auto kRealisticJson5Document = R"json5(
/*
 * Fictional Game Configuration File
 * This document demonstrates all major JSON5 syntax features.
 * Unlike standard JSON, JSON5 allows multi-line comments.
 */
{
  // 1. Unquoted Keys: Object keys don't need quotes if they are valid identifiers
  title: 'Epic Fantasy Adventure',

  // 2. Single Quotes: Strings can be wrapped in single quotes
  environment: 'production',
  isDemo: false,

  // 3. Multi-line Strings: Strings can span multiple lines by escaping the newline
  description: 'This is a significantly long description \
that spans across multiple lines in the document \
to cleanly demonstrate JSON5 string continuations \
without needing explicit \\n characters in the source.',

  // 4. Numeric formats: JSON5 supports several new ways to write numbers
  stats: {
    maxPlayers: +8,          // Explicit positive sign
    baseHealth: 150.,        // Trailing decimal point
    damageMultiplier: .75,   // Leading decimal point
    magicHexCode: 0xCAFEBABE,// Hexadecimal numbers
    levelCap: Infinity,      // Positive Infinity
    negativeLimit: -Infinity,// Negative Infinity
    unknownValue: NaN,       // Not a Number (NaN)
  },                         // <-- Trailing comma in object!

  // 5. Arrays with trailing commas
  features: [
    'Single-player',
    'Multi-player',
    'Co-op',                 // <-- Trailing comma in array!
  ],

  // 6. Nested structures mixing standard JSON and JSON5 syntax
  characters: {
    'Hero': {
      class: 'Paladin',
      inventory: ['sword', 'shield', 'potion',],
    },
    "Villain": {
      class: "Necromancer",
      inventory: ["staff", "skull"], // Standard JSON still perfectly valid
    },
  },

  /*
   * 7. Additional configuration to expand file size.
   * JSON5 ignores all this extra whitespace and formatting.
   */
  networkConfig: {
    timeoutMs: 5000.,
    retryCount: +3,
    useProxy: false,
    endpoints: [
      'https://api.game-server-1.com/v1',
      'https://api.game-server-2.com/v1',
    ],
  },
}
)json5";

constexpr auto kRealisticJson5DocumentConvertedWithoutWhitespace =
    R"json({"title":"Epic Fantasy Adventure","environment":"production","isDemo":false,"description":"This is a significantly long description that spans across multiple lines in the document to cleanly demonstrate JSON5 string continuations without needing explicit \\n characters in the source.","stats":{"maxPlayers":8,"baseHealth":150.0,"damageMultiplier":0.75,"magicHexCode":3405691582,"levelCap":Infinity,"negativeLimit":-Infinity,"unknownValue":NaN},"features":["Single-player","Multi-player","Co-op"],"characters":{"Hero":{"class":"Paladin","inventory":["sword","shield","potion"]},"Villain":{"class":"Necromancer","inventory":["staff","skull"]}},"networkConfig":{"timeoutMs":5000.0,"retryCount":3,"useProxy":false,"endpoints":["https://api.game-server-1.com/v1","https://api.game-server-2.com/v1"]}})json";

TEST(JsonStreamTest, SalvageStreamProcessorRealisticJson5Document) {
  EXPECT_EQ(RunSalvage(kRealisticJson5Document, kAllowNanJsonSalvageOptions),
            kRealisticJson5DocumentConvertedWithoutWhitespace);
}

TEST(JsonStreamTest, SalvageStreamProcessorValidJson5) {
  EXPECT_EQ(RunSalvage("// a comment\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\n// another comment\n123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r// another comment\r123"), "123");
  EXPECT_EQ(RunSalvage("// a comment\r\n// another comment\r\n123"), "123");

  EXPECT_EQ(RunSalvage("/* a comment\n and **another** comment*/123"), "123");

  EXPECT_EQ(RunSalvage(".0"), "0.0");
  EXPECT_EQ(RunSalvage("0."), "0.0");
  EXPECT_EQ(RunSalvage("1."), "1.0");
  EXPECT_EQ(RunSalvage("-.0"), "-0.0");
  EXPECT_EQ(RunSalvage("+.0"), "0.0");
  EXPECT_EQ(RunSalvage("-0."), "-0.0");
  EXPECT_EQ(RunSalvage("-1."), "-1.0");
  EXPECT_EQ(RunSalvage("+0."), "0.0");
  EXPECT_EQ(RunSalvage("+1."), "1.0");

  EXPECT_EQ(RunSalvage("0x1234"), "4660");
  EXPECT_EQ(RunSalvage("-0x1234"), "-4660");
  EXPECT_EQ(RunSalvage("0X1234"), "4660");

  EXPECT_EQ(RunSalvage("0x123456789abcdef"), "81985529216486895");
  EXPECT_EQ(RunSalvage("0x123456789aBcDeF"), "81985529216486895");
  EXPECT_EQ(RunSalvage("0x123456789ABCDEF"), "81985529216486895");

  EXPECT_EQ(RunSalvage("-Infinity"), "\"-Infinity\"");
  EXPECT_EQ(RunSalvage("+Infinity"), "\"+Infinity\"");
  EXPECT_EQ(RunSalvage("Infinity"), "\"Infinity\"");
  EXPECT_EQ(RunSalvage("NaN"), "\"NaN\"");
  EXPECT_EQ(RunSalvage("-NaN"), "\"-NaN\"");
  EXPECT_EQ(RunSalvage("+NaN"), "\"+NaN\"");

  EXPECT_EQ(RunSalvage("-Infinity", kAllowNanJsonSalvageOptions), "-Infinity");
  EXPECT_EQ(RunSalvage("+Infinity", kAllowNanJsonSalvageOptions), "Infinity");
  EXPECT_EQ(RunSalvage("Infinity", kAllowNanJsonSalvageOptions), "Infinity");
  EXPECT_EQ(RunSalvage("NaN", kAllowNanJsonSalvageOptions), "NaN");
  EXPECT_EQ(RunSalvage("-NaN", kAllowNanJsonSalvageOptions), "NaN");
  EXPECT_EQ(RunSalvage("+NaN", kAllowNanJsonSalvageOptions), "NaN");

  EXPECT_EQ(RunSalvage("''"), "\"\"");
  EXPECT_EQ(RunSalvage("' '"), "\" \"");
  EXPECT_EQ(RunSalvage("'\"'"), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("'\\''"), "\"'\"");
  EXPECT_EQ(RunSalvage("'' ''"), "\"\"\n\"\"");

  // https://spec.json5.org/#escapes (for Line Terminators)
  EXPECT_EQ(RunSalvage("'a\\\nb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\rb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\r\nb'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\u2028b'"), "\"ab\"");
  EXPECT_EQ(RunSalvage("'a\\\u2029b'"), "\"ab\"");

  EXPECT_EQ(RunSalvage("\"\\x42\""), "\"B\"");
  EXPECT_EQ(RunSalvage("\"\\x12\""), "\"\\u0012\"");
  EXPECT_EQ(RunSalvage("\"\\0\""), "\"\\u0000\"");

  EXPECT_EQ(RunSalvage("[1,2,3,]"), "[1,2,3]");

  EXPECT_EQ(RunSalvage("{foo:true,bar:false}"), "{\"foo\":true,\"bar\":false}");

  // https://spec.json5.org/#white-space
  EXPECT_EQ(RunSalvage("123\u0009456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000a456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000b456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000c456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u000d456"), "123\n456");
  EXPECT_EQ(RunSalvage("123 456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u00a0456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2028456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2029456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\ufeff456"), "123\n456");
  EXPECT_EQ(RunSalvage("123\u2009456"), "123\n456");  // U+2009 THIN SPACE
  EXPECT_EQ(RunSalvage("123\u3000456"),
            "123\n456");  // U+2000 IDEOGRAPHIC SPACE
}

TEST(JsonStreamTest, SalvageStreamProcessorValidPythonOrJavascript) {
  EXPECT_EQ(RunSalvage("# a comment\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\n# another comment\n123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r# another comment\r123"), "123");
  EXPECT_EQ(RunSalvage("# a comment\r\n# another comment\r\n123"), "123");

  EXPECT_EQ(RunSalvage("None"), "null");
  EXPECT_EQ(RunSalvage("True"), "true");
  EXPECT_EQ(RunSalvage("False"), "false");

  EXPECT_EQ(RunSalvage("0"), "0");
  EXPECT_EQ(RunSalvage("00"), "0");
  EXPECT_EQ(RunSalvage("000"), "0");
  EXPECT_EQ(RunSalvage("0_00"), "0");
  EXPECT_EQ(RunSalvage("00_0"), "0");
  EXPECT_EQ(RunSalvage("000_"), "0");

  EXPECT_EQ(RunSalvage("0x_1234"), "4660");
  EXPECT_EQ(RunSalvage("0x12_34"), "4660");
  EXPECT_EQ(RunSalvage("0x1234_"), "4660");
  EXPECT_EQ(RunSalvage("0xDead_beef"), "3735928559");

  EXPECT_EQ(RunSalvage("3.14_15_93"), "3.141593");

  EXPECT_EQ(RunSalvage("0o123"), "83");
  EXPECT_EQ(RunSalvage("-0o123"), "-83");
  EXPECT_EQ(RunSalvage("0o_123"), "83");
  EXPECT_EQ(RunSalvage("0o1_23"), "83");
  EXPECT_EQ(RunSalvage("0o123_"), "83");
  EXPECT_EQ(RunSalvage("0o01234567"), "342391");

  EXPECT_EQ(RunSalvage("0b1011"), "11");
  EXPECT_EQ(RunSalvage("-0b1011"), "-11");
  EXPECT_EQ(RunSalvage("0b_1011"), "11");
  EXPECT_EQ(RunSalvage("0b10_11"), "11");
  EXPECT_EQ(RunSalvage("0b1011_"), "11");

  EXPECT_EQ(RunSalvage("123n"), "123");
  EXPECT_EQ(RunSalvage("123l"), "123");
  EXPECT_EQ(RunSalvage("123L"), "123");
  EXPECT_EQ(RunSalvage("0x123n"), "291");
  EXPECT_EQ(RunSalvage("0x123l"), "291");
  EXPECT_EQ(RunSalvage("0x123L"), "291");
  EXPECT_EQ(RunSalvage("0o123n"), "83");
  EXPECT_EQ(RunSalvage("0o123l"), "83");
  EXPECT_EQ(RunSalvage("0o123L"), "83");

  EXPECT_EQ(RunSalvage("\"\b\f\n\r\t\""), "\"\\b\\f\\n\\r\\t\"");

  EXPECT_EQ(RunSalvage("``"), "\"\"");
  EXPECT_EQ(RunSalvage("` `"), "\" \"");
  EXPECT_EQ(RunSalvage("`\"`"), "\"\\\"\"");
  EXPECT_EQ(RunSalvage("`\\``"), "\"`\"");

  EXPECT_EQ(RunSalvage("''''''"), "\"\"");
  EXPECT_EQ(RunSalvage("''' '''"), "\" \"");
  EXPECT_EQ(RunSalvage("''' ' '''"), "\" ' \"");
  EXPECT_EQ(RunSalvage("''' '' '''"), "\" '' \"");
  EXPECT_EQ(RunSalvage("'''\n\n\n'''"), "\"\\n\\n\\n\"");
  EXPECT_EQ(RunSalvage("'''a\\\nb'''"), "\"ab\"");

  EXPECT_EQ(RunSalvage("\"\"\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\" \"\"\""), "\" \"");
  EXPECT_EQ(RunSalvage("\"\"\" \" \"\"\""), "\" \\\" \"");
  EXPECT_EQ(RunSalvage("\"\"\" \"\" \"\"\""), "\" \\\"\\\" \"");
  EXPECT_EQ(RunSalvage("\"\"\"\n\n\n\"\"\""), "\"\\n\\n\\n\"");

  EXPECT_EQ(RunSalvage("b''"), "\"\"");
  EXPECT_EQ(RunSalvage("b'abc'"), "\"abc\"");
  EXPECT_EQ(RunSalvage("b'\\x00\\xFF'"), "\"\\u0000\u00ff\"");
  EXPECT_EQ(RunSalvage("b'\\x00\\xFF'", kEnsureAsciiJsonSalvageOptions),
            "\"\\u0000\\u00ff\"");

  EXPECT_EQ(RunSalvage("u''"), "\"\"");
  EXPECT_EQ(RunSalvage("u'abc'"), "\"abc\"");

  EXPECT_EQ(RunSalvage("b'''abc'''"), "\"abc\"");
  EXPECT_EQ(RunSalvage("b\"\"\"abc\"\"\""), "\"abc\"");
  EXPECT_EQ(RunSalvage("u'''abc'''"), "\"abc\"");
  EXPECT_EQ(RunSalvage("u\"\"\"abc\"\"\""), "\"abc\"");

  EXPECT_EQ(RunSalvage("\"\\\\'\\\"\\a\\b\\f\\n\\r\\t\\v\""),
            "\"\\\\'\\\"\\u0007\\b\\f\\n\\r\\t\\u000b\"");

  EXPECT_EQ(RunSalvage("\"\\1\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\00\""), "\"\\u0000\"");
  EXPECT_EQ(RunSalvage("\"\\01\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\21\""), "\"\\u0011\"");
  EXPECT_EQ(RunSalvage("\"\\000\""), "\"\\u0000\"");
  EXPECT_EQ(RunSalvage("\"\\001\""), "\"\\u0001\"");
  EXPECT_EQ(RunSalvage("\"\\021\""), "\"\\u0011\"");
  EXPECT_EQ(RunSalvage("\"\\321\""), "\"\u00d1\"");
  EXPECT_EQ(RunSalvage("\"\\U0001f60a\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\u{1f60a}\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\u{01f60a}\""), "\"😊\"");

  EXPECT_EQ(RunSalvage("(1,2,3)"), "[1,2,3]");
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidUtf8) {
  EXPECT_EQ(RunSalvage("\xff"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\xc2\xff"), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\xed\xa0\x80"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\x80"), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\xc2"), "");  // Truncated by end of input.
  EXPECT_EQ(RunSalvage("\xc2\xc2"),
            "\"\ufffd\"");  // Truncated by end of input.
  EXPECT_EQ(RunSalvage("\xc0\x80"), "\"\ufffd\ufffd\"");    // Overlong.
  EXPECT_EQ(RunSalvage("\xe0\x8f\x80"), "\"\ufffd\"");      // Overlong.
  EXPECT_EQ(RunSalvage("\xf0\x8f\x80\x80"), "\"\ufffd\"");  // Overlong.
  EXPECT_EQ(RunSalvage("\xed\xa0\x80"), "\"\ufffd\"");      // Surrogate.

  EXPECT_EQ(RunSalvage("\"\xff\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\xff\""), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\x80\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc2\xc2\""), "\"\ufffd\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\xc0\x80\""), "\"\ufffd\ufffd\"");  // Overlong.
  EXPECT_EQ(RunSalvage("\"\xe0\x8f\x80\""), "\"\ufffd\"");    // Overlong.
  EXPECT_EQ(RunSalvage("\"\xed\xa0\x80\""), "\"\ufffd\"");    // Surrogate.

  EXPECT_EQ(RunSalvage("[\xff]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2\xff]"), "[\"\ufffd\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\x80]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2]"), "[\"\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc2\xc2]"), "[\"\ufffd\ufffd\"]");
  EXPECT_EQ(RunSalvage("[\xc0\x80]"), "[\"\ufffd\ufffd\"]");  // Overlong.
  EXPECT_EQ(RunSalvage("[\xe0\x8f\x80]"), "[\"\ufffd\"]");    // Overlong.
  EXPECT_EQ(RunSalvage("[\xed\xa0\x80]"), "[\"\ufffd\"]");    // Surrogate.
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidUtf16Surrogates) {
  EXPECT_EQ(RunSalvage("\"\\ud800\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\ud800?\""), "\"?\ufffd?\"");
  EXPECT_EQ(RunSalvage("\"\\udc00\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\udc00?\""), "\"?\ufffd?\"");

  EXPECT_EQ(RunSalvage("\"\\ud800\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\ud800?\"", kEnsureAsciiJsonSalvageOptions),
            "\"?\\ufffd?\"");
  EXPECT_EQ(RunSalvage("\"\\udc00\"", kEnsureAsciiJsonSalvageOptions),
            "\"\\ufffd\"");
  EXPECT_EQ(RunSalvage("\"?\\udc00?\"", kEnsureAsciiJsonSalvageOptions),
            "\"?\\ufffd?\"");
}

TEST(JsonStreamTest, SalvageStreamProcessorInvalidFormat) {
  EXPECT_EQ(RunSalvage("{"), "{}");
  EXPECT_EQ(RunSalvage("}"), "");
  EXPECT_EQ(RunSalvage("{}}"), "{}");
  EXPECT_EQ(RunSalvage("{}}}"), "{}");
  EXPECT_EQ(RunSalvage("{\"x\":"), "{\"x\":null}");
  EXPECT_EQ(RunSalvage("["), "[]");
  EXPECT_EQ(RunSalvage("]"), "");
  EXPECT_EQ(RunSalvage("[]]"), "[]");
  EXPECT_EQ(RunSalvage("[]]]"), "[]");
  EXPECT_EQ(RunSalvage("[1"), "[1]");
  EXPECT_EQ(RunSalvage("[1,2"), "[1,2]");
  EXPECT_EQ(RunSalvage("\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("\"\"\"\"\""), "\"\"");
  EXPECT_EQ(RunSalvage("'"), "\"\"");
  EXPECT_EQ(RunSalvage("'''"), "\"\"");
  EXPECT_EQ(RunSalvage("''''"), "\"\"");
  EXPECT_EQ(RunSalvage("'''''"), "\"\"");
  EXPECT_EQ(RunSalvage("`"), "\"\"");
  EXPECT_EQ(RunSalvage("\"\\"), "\"\"");
  EXPECT_EQ(RunSalvage("."), "0.0");
  EXPECT_EQ(RunSalvage(".e"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.0e"), "0.0e0");
  EXPECT_EQ(RunSalvage("0.e"), "0.0e0");
  EXPECT_EQ(RunSalvage("#"), "");
  EXPECT_EQ(RunSalvage("//"), "");
  EXPECT_EQ(RunSalvage("/*"), "");
  EXPECT_EQ(RunSalvage("/* *"), "");
  EXPECT_EQ(RunSalvage("+/"), "\"+/\"");

  EXPECT_EQ(RunSalvage("00123"), "123");
  EXPECT_EQ(RunSalvage("0b01012"), "5\n2");
  EXPECT_EQ(RunSalvage("0o123456789"), "342391\n89");

  EXPECT_EQ(RunSalvage(".."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.0."), "0.0\n0.0");
  EXPECT_EQ(RunSalvage("0.e."), "0.0e0\n0.0");
  EXPECT_EQ(RunSalvage("0.ee"), "0.0e0\n\"e\"");
  EXPECT_EQ(RunSalvage("000'"), "0");
  EXPECT_EQ(RunSalvage("0'00"), "0");
  EXPECT_EQ(RunSalvage("00'0"), "0");
  EXPECT_EQ(RunSalvage("000'"), "0");
  EXPECT_EQ(RunSalvage("00."), "0.0");
  EXPECT_EQ(RunSalvage("000."), "0.0");

  EXPECT_EQ(RunSalvage("/"), "\"/\"");
  EXPECT_EQ(RunSalvage("[/]"), "[\"/\"]");
  EXPECT_EQ(RunSalvage("/abc"), "\"/abc\"");
  EXPECT_EQ(RunSalvage("[/abc]"), "[\"/abc\"]");

  EXPECT_EQ(RunSalvage("t0"), "\"t0\"");
  EXPECT_EQ(RunSalvage("tr0"), "\"tr0\"");
  EXPECT_EQ(RunSalvage("tru0"), "\"tru0\"");
  EXPECT_EQ(RunSalvage("true0"), "true\n0");
  EXPECT_EQ(RunSalvage("t."), "\"t.\"");
  EXPECT_EQ(RunSalvage("tr."), "\"tr.\"");
  EXPECT_EQ(RunSalvage("tru."), "\"tru.\"");
  EXPECT_EQ(RunSalvage("true."), "true\n0.0");

  EXPECT_EQ(RunSalvage("abc[def"), "\"abc\"\n[\"def\"]");
  EXPECT_EQ(RunSalvage("abc{def"), "\"abc\"\n{\"def\":null}");
  EXPECT_EQ(RunSalvage("[abc[def"), "[\"abc\",[\"def\"]]");

  EXPECT_EQ(RunSalvage("\"\\w\""), "\"\\\\w\"");  // Not a real escape.
  EXPECT_EQ(RunSalvage("\"\\u42???\""), "\"B???\"");
  EXPECT_EQ(RunSalvage("\"\\xf???\""), "\"\\u000f???\"");
  EXPECT_EQ(RunSalvage("\"\\U1f60a???\""), "\"😊???\"");
  EXPECT_EQ(RunSalvage("\"\\U{1f60a}\""), "\"😊\"");
  EXPECT_EQ(RunSalvage("\"\\Uaaaaaaaa\""), "\"\ufffd\"");
  EXPECT_EQ(RunSalvage("\"\\u{aaaaaaaa}\""), "\"\ufffd\"");

  EXPECT_EQ(RunSalvage("{\"x\"}"), "{\"x\":null}");
  EXPECT_EQ(RunSalvage("{[1]:[2]}"), "{\"\":[1],\"\":[2]}");
  EXPECT_EQ(RunSalvage("{[1],[2]}"), "{\"\":[1],\"\":[2]}");
  EXPECT_EQ(RunSalvage("{{1}:{2}}"), "{\"\":{\"1\":null},\"\":{\"2\":null}}");
  EXPECT_EQ(RunSalvage("{[1]:\"a\"}"), "{\"\":[1],\"a\":null}");

  EXPECT_EQ(RunSalvage("[1 2 3]"), "[1,2,3]");
  EXPECT_EQ(RunSalvage("[,, ,1,,2,, ,,3,,]"), "[1,2,3]");
  EXPECT_EQ(RunSalvage("[,,,1, ,2: ::3,, ]"), "[1,2,3]");

  // Truncated to lower 64 bits.
  EXPECT_EQ(RunSalvage("0x123456789abcdef01"), "2541551405711093505");
  EXPECT_EQ(RunSalvage("0o12345670123456701234567"), "4140784126758566263");
  EXPECT_EQ(RunSalvage("0b01010101010101010101010101010101010101010101010101010"
                       "1010101010101"),
            "6148914691236517205");

  EXPECT_EQ(
      RunSalvage(
          // 200 '['s
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
          "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["),
      // Beyond max depth, treated as unquoted string, but still matched
      // fully afterward.
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["
      "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"]]]]]]]]]]]]"
      "]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"
      "]]]]]]]]]]]]]]]]");

  // Markdown-style code block fences aren't treated as backtick strings, and
  // are instead treated as unquoted strings containing only the ``` lines, so
  // that the content isn't captured as a single string instead of being parsed.
  EXPECT_EQ(RunSalvage("```json\n{\"x\": \"y\"}\n```\n"),
            "\"```json\"\n{\"x\":\"y\"}\n\"```\"");

  // Various "torture test" inputs contributed by a supernatural entity.
  EXPECT_EQ(RunSalvage("nulla[truep,Falsen,{1:None1}]"),
            "null\n\"a\"\n[true,\"p\",false,\"n\",{\"1\":null,\"1\":null}]");
  EXPECT_EQ(RunSalvage("\"'nested \\\" \\' \\`'\""), "\"'nested \\\" ' `'\"");
  EXPECT_EQ(RunSalvage("'foo\\\nbar' \\\n \"\"\"baz\\\rquux\"\"\""),
            "\"foobar\"\n\"\\\\\"\n\"bazquux\"");
  EXPECT_EQ(RunSalvage("foo'bar'`baz`\"quux\""),
            "\"foo'bar'`baz`\\\"quux\\\"\"");
  EXPECT_EQ(RunSalvage("1/*foo*/2"), "1\n2");
  EXPECT_EQ(RunSalvage("/* // foo */ bar // /* baz */"), "\"bar\"");
  EXPECT_EQ(RunSalvage("\x80 \xc0\x80 \xf0\x8f\x80\x80"),
            "\"\ufffd\"\n\"\ufffd\ufffd\"\n\"\ufffd\"");
}

TEST(JsonStreamTest, SalvageStreamProcessorMinimallyDelayed) {
  JsonSalvageStreamProcessor processor(kDefaultJsonSalvageOptions);
  // {"a": 123, "b\u2660c": ['''xyz''', 12.34e56]}

  // Note: separators (',' ':') are delayed slightly because it's impossible to
  // do removal of trailing commas without waiting for the next value or end of
  // container.
  EXPECT_EQ(processor.ProcessInputChunk("{"), "{");
  EXPECT_EQ(processor.ProcessInputChunk("\""), "\"");
  EXPECT_EQ(processor.ProcessInputChunk("a"), "a");
  EXPECT_EQ(processor.ProcessInputChunk("\""), "\"");
  EXPECT_EQ(processor.ProcessInputChunk(":"), "");
  EXPECT_EQ(processor.ProcessInputChunk(" "), "");
  EXPECT_EQ(processor.ProcessInputChunk("1"), ":1");
  EXPECT_EQ(processor.ProcessInputChunk("2"), "2");
  EXPECT_EQ(processor.ProcessInputChunk("3"), "3");
  EXPECT_EQ(processor.ProcessInputChunk(","), "");
  EXPECT_EQ(processor.ProcessInputChunk(" "), "");
  EXPECT_EQ(processor.ProcessInputChunk("\""), ",\"");
  EXPECT_EQ(processor.ProcessInputChunk("b"), "b");
  EXPECT_EQ(processor.ProcessInputChunk("\\"), "");
  EXPECT_EQ(processor.ProcessInputChunk("u"), "");
  EXPECT_EQ(processor.ProcessInputChunk("2"), "");
  EXPECT_EQ(processor.ProcessInputChunk("6"), "");
  EXPECT_EQ(processor.ProcessInputChunk("6"), "");
  EXPECT_EQ(processor.ProcessInputChunk("0"), "♠");
  EXPECT_EQ(processor.ProcessInputChunk("c"), "c");
  EXPECT_EQ(processor.ProcessInputChunk("\""), "\"");
  EXPECT_EQ(processor.ProcessInputChunk(":"), "");
  EXPECT_EQ(processor.ProcessInputChunk(" "), "");
  EXPECT_EQ(processor.ProcessInputChunk("["), ":[");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "\"");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "");
  EXPECT_EQ(processor.ProcessInputChunk("x"), "x");
  EXPECT_EQ(processor.ProcessInputChunk("y"), "y");
  EXPECT_EQ(processor.ProcessInputChunk("z"), "z");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "");
  EXPECT_EQ(processor.ProcessInputChunk("'"), "\"");
  EXPECT_EQ(processor.ProcessInputChunk(","), "");
  EXPECT_EQ(processor.ProcessInputChunk(" "), "");
  EXPECT_EQ(processor.ProcessInputChunk("1"), ",1");
  EXPECT_EQ(processor.ProcessInputChunk("2"), "2");
  EXPECT_EQ(processor.ProcessInputChunk("."), ".");
  EXPECT_EQ(processor.ProcessInputChunk("3"), "3");
  EXPECT_EQ(processor.ProcessInputChunk("4"), "4");
  EXPECT_EQ(processor.ProcessInputChunk("e"), "e");
  EXPECT_EQ(processor.ProcessInputChunk("5"), "5");
  EXPECT_EQ(processor.ProcessInputChunk("6"), "6");
  EXPECT_EQ(processor.ProcessInputChunk("]"), "]");
  EXPECT_EQ(processor.ProcessInputChunk("}"), "}");
}

TEST(JsonStreamTest, SalvageStreamProcessorLoadStateFailure) {
  JsonSalvageStreamProcessor processor(kDefaultJsonSalvageOptions);

  EXPECT_FALSE(processor.LoadState(""));
  EXPECT_FALSE(processor.LoadState("GARBAGE"));
  EXPECT_FALSE(processor.LoadState(
      JsonSalvageStreamProcessor(kAllowNanJsonSalvageOptions).ToState()));

  {
    JsonSalvageStateProto proto;
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 4; ++i) {
      proto.mutable_utf8_buffer()->push_back('A');
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 101; ++i) {
      proto.add_container_stack(0);
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_container_stack(3);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_container_stack(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    for (int i = 0; i < 101; ++i) {
      proto.add_buffer(0);
    }
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_buffer(0x110000);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.add_buffer(-1);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
  {
    processor.Reset();
    JsonSalvageStateProto proto;
    proto.ParseFromString(processor.ToState());
    proto.set_state(11);
    EXPECT_FALSE(processor.LoadState(proto.SerializeAsString()));
  }
}

}  // namespace
}  // namespace koladata::internal
