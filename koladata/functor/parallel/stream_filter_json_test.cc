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
#include "koladata/functor/parallel/stream_filter_json.h"

#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/strings/string_view.h"

namespace koladata::functor::parallel {
namespace {

using ::absl_testing::IsOk;
using ::absl_testing::StatusIs;
using ::testing::ElementsAre;
using ::testing::Pair;

TEST(JsonStreamParserTest, Basic) {
  JsonStreamParser parser;
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": 1, "b": "foo", "c": true, "d": null})"),
              IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(
      values,
      ElementsAre(Pair("$.a", "1"), Pair("$.b", "\"foo\""), Pair("$.c", "true"),
                  Pair("$.d", "null"),
                  Pair("$", R"({"a": 1, "b": "foo", "c": true, "d": null})")));
}

TEST(JsonStreamParserTest, Nested) {
  JsonStreamParser parser;
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": {"b": [1, 2]}})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values, ElementsAre(Pair("$.a.b[*]", "1"), Pair("$.a.b[*]", "2"),
                                  Pair("$.a.b", "[1, 2]"),
                                  Pair("$.a", R"({"b": [1, 2]})"),
                                  Pair("$", R"({"a": {"b": [1, 2]}})")));
}

TEST(JsonStreamParserTest, PathTracking) {
  JsonStreamParser parser;
  std::vector<std::string> paths;
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        paths.push_back(std::string(path));
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": {"b": [1]}})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(paths, ElementsAre("$", "$.a", "$.a.b", "$.a.b[*]"));
}

TEST(JsonStreamParserTest, PerCharacterInput) {
  JsonStreamParser parser;
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  const std::string json = R"({"a": 123, "b": 456})";
  for (char c : json) {
    EXPECT_THAT(parser.AddChunk(absl::string_view(&c, 1)), IsOk());
  }
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values, ElementsAre(Pair("$.a", "123"), Pair("$.b", "456"),
                                  Pair("$", json)));
}

TEST(JsonStreamParserTest, Values) {
  JsonStreamParser parser;
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path != "$") {
          values.push_back({std::string(path), std::string(json)});
        }
      });

  EXPECT_THAT(parser.AddChunk(R"({"s": "str\"ing", "n": 1.23, "b": false})"),
              IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values, ElementsAre(Pair("$.s", R"("str\"ing")"),
                                  Pair("$.n", "1.23"), Pair("$.b", "false")));
}

TEST(JsonStreamParserTest, ConsumeChunkKeep) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::KEEP);
  std::string consumed;
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        if (path == "$.a[*]") {
          parser.ConsumeChunk();
        }
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path == "$.a[*]") {
          if (!consumed.empty()) consumed += ", ";
          consumed += parser.ConsumeChunk();
        }
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": [1, 2, 3]})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_EQ(consumed, "1, 2, 3");
}

TEST(JsonStreamParserTest, ConsumeChunkRemove) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
  std::string consumed;
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        if (path == "$.a[*]") {
          parser.ConsumeChunk();
        }
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path == "$.a[*]") {
          if (!consumed.empty()) consumed += ",";
          consumed += parser.ConsumeChunk();
        }
      });

  EXPECT_THAT(parser.AddChunk("{\"a\": [1,\n 2,\t 3 ]}"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_EQ(consumed, "1,2,3");
}

TEST(JsonStreamParserTest, SeparatorPolicyRemoveInStrings) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": "foo bar"})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  // REMOVE policy should not affect spaces within string literals.
  EXPECT_THAT(values, ElementsAre(Pair("$.a", "\"foo bar\""),
                                  Pair("$", R"({"a":"foo bar"})")));
}

TEST(JsonStreamParserTest, OutputStreaming) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
  std::string filter = "$.query.docs[*]";
  bool streaming = false;
  std::string output;
  std::vector<std::string> docs;

  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        if (path == filter && type == JsonStreamParser::ValueType::DICT) {
          parser.ConsumeChunk();  // flush previous values
          streaming = true;
        }
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path == filter && streaming) {
          docs.push_back(output + std::string(parser.ConsumeChunk()));
          output.clear();
          streaming = false;
        }
      });

  std::string json = R"({"query": {"docs": [{"id": 1}, {"id": 2}]}})";
  // Send bytes in small chunks.
  for (int i = 0; i < json.size(); i += 3) {
    EXPECT_THAT(parser.AddChunk(json.substr(i, 3)), IsOk());
    if (streaming) {
      output += parser.ConsumeChunk();
    }
  }
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(docs, ElementsAre(R"({"id":1})", R"({"id":2})"));
}

TEST(JsonStreamParserTest, ConcatenatedJson) {
  JsonStreamParser parser;
  std::vector<std::string> top_level_values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path == "$") {
          top_level_values.push_back(std::string(json));
        }
      });

  EXPECT_THAT(parser.AddChunk(R"({"a": 1} {"b": 2})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(top_level_values, ElementsAre(R"({"a": 1})", R"({"b": 2})"));
}

TEST(JsonStreamParserTest, NotFailingOnMarkdownBacktics) {
  JsonStreamParser parser;
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  // Parser interprets "`" as a letter and classifies "```json", "```" same way
  // as tokens like true/false. It is not a valid JSON, but can happen in
  // practice, so we intentionally don't fail on it.
  EXPECT_THAT(parser.AddChunk(R"(
```json
{"a": 1}
```
)"),
              IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values, ElementsAre(Pair("$", "```json"), Pair("$.a", "1"),
                                  Pair("$", R"({"a": 1})"), Pair("$", "```")));
}

TEST(JsonStreamParserTest, FinalizeError) {
  JsonStreamParser parser;
  EXPECT_THAT(parser.AddChunk(R"({"a": 1)"), IsOk());
  EXPECT_THAT(parser.Finalize(), StatusIs(absl::StatusCode::kInvalidArgument,
                                          "unexpected end of json stream"));
}

TEST(JsonStreamParserTest, InvalidJson) {
  JsonStreamParser parser;
  EXPECT_THAT(parser.AddChunk(R"({"a": })"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected value begin, got '}' at pos 6"));

  parser = JsonStreamParser();
  EXPECT_THAT(
      parser.AddChunk(R"({"a": 1] )"),
      StatusIs(absl::StatusCode::kInvalidArgument, "unexpected ']' at pos 7"));
  parser = JsonStreamParser();
  EXPECT_THAT(parser.AddChunk(R"({"a": 1, "b")"), IsOk());
  EXPECT_THAT(parser.AddChunk(R"( : 2, "c"  3})"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected ':' at pos 23, got '3'"));
  parser = JsonStreamParser();
  // single quotes off by default
  EXPECT_THAT(parser.AddChunk(R"({'a': 1})"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "single quote instead of double quote at pos 1"));

  parser = JsonStreamParser();
  // unquoted keys off by default
  EXPECT_THAT(parser.AddChunk(R"({a: 1})"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "expected '\"' at pos 1, got 'a'"));

  parser = JsonStreamParser();
  // linebreaks off by default
  EXPECT_THAT(parser.AddChunk("{\"a\": \"foo\nbar\"}"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "linebreak inside of string literal at pos 10"));
}

TEST(JsonStreamParserTest, AllowSingleQuotes) {
  JsonStreamParser parser;
  parser.AllowSingleQuotes(true);
  std::vector<std::pair<std::string, std::string>> values;
  std::vector<JsonStreamParser::ValueType> types;
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        types.push_back(type);
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk(R"({'ab': 'foo', "b": 'bar'})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values,
              ElementsAre(Pair("$.ab", "\"foo\""), Pair("$.b", "\"bar\""),
                          Pair("$", R"({"ab": "foo", "b": "bar"})")));
  EXPECT_THAT(types, ElementsAre(JsonStreamParser::ValueType::DICT,
                                 JsonStreamParser::ValueType::STRING,
                                 JsonStreamParser::ValueType::STRING));
}

TEST(JsonStreamParserTest, AllowUnquotedKeys) {
  JsonStreamParser parser;
  parser.AllowUnquotedKeys(true);
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk(R"({a: 1, bc : 2})"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values, ElementsAre(Pair("$.a", "1"), Pair("$.bc", "2"),
                                  Pair("$", R"({"a": 1, "bc" : 2})")));
}

TEST(JsonStreamParserTest, AllowLinebreakInStrings) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
  parser.AllowLinebreakInStrings(true);
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk("{\"a\": \"foo\nbar\", \"b\": \"baz\r\"}"),
              IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(
      values,
      ElementsAre(Pair("$.a", "\"foo\\nbar\""), Pair("$.b", "\"baz\\r\""),
                  Pair("$", R"({"a":"foo\nbar","b":"baz\r"})")));
}

TEST(JsonStreamParserTest, EnableQuotingInvalidTokens) {
  JsonStreamParser parser;
  parser.EnableQuotingInvalidTokens(true);
  std::vector<std::pair<std::string, std::string>> values;
  std::vector<JsonStreamParser::ValueType> types;
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        if (path == "$.a" || path == "$.b" || path == "$.c") {
          types.push_back(type);
        }
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(
      parser.AddChunk(R"({"a": true, "b": 3.14e-2, "c": yes, "d": 0XfF1})"),
      IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(
      values,
      ElementsAre(
          Pair("$.a", "true"), Pair("$.b", "3.14e-2"), Pair("$.c", "\"yes\""),
          Pair("$.d", "0XfF1"),
          Pair("$", R"({"a": true, "b": 3.14e-2, "c": "yes", "d": 0XfF1})")));
  EXPECT_THAT(types, ElementsAre(JsonStreamParser::ValueType::TOKEN,
                                 JsonStreamParser::ValueType::TOKEN,
                                 JsonStreamParser::ValueType::STRING));
}

TEST(JsonStreamParserTest, TokenParsing) {
  JsonStreamParser parser;
  parser.EnableQuotingInvalidTokens(true);
  std::vector<std::string> quoted;
  std::vector<std::string> unquoted;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (json.size() >= 2 && json.front() == '"' && json.back() == '"') {
          quoted.emplace_back(json.substr(1, json.size() - 2));
        } else {
          unquoted.emplace_back(json);
        }
      });
  EXPECT_THAT(parser.AddChunk(R"(
    true false null True False Null yes no none None
    NaN NAN -inf INF +Infinity
    123 123ABC A5 0xa5 0XA5 -0xa5 0xa5p-3
    0xffffffffffffffff 0xffffffffffffffff11 0xffffffffffffffffp10
    123-5 123g-5 123e-5 12.34e7 12.34.56 -10000000000 100000000000000000000
    -1e10000000000000000000000000000
    )"),
              IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  // IsValidToken -> true
  EXPECT_THAT(
      unquoted,
      ElementsAre("true", "false", "null", "NaN", "NAN", "-inf", "INF",
                  "+Infinity", "123", "0xa5", "0XA5", "-0xa5", "0xa5p-3",
                  "0xffffffffffffffff", "0xffffffffffffffff11",
                  "0xffffffffffffffffp10", "123e-5", "12.34e7", "-10000000000",
                  "100000000000000000000", "-1e10000000000000000000000000000"));

  // IsValidToken -> false
  EXPECT_THAT(quoted,
              ElementsAre("True", "False", "Null", "yes", "no", "none", "None",
                          "123ABC", "A5", "123-5", "123g-5", "12.34.56"));
}

TEST(JsonStreamParserTest, WithAllCompatibilityOptions) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::KEEP);
  parser.AllowSingleQuotes(true);
  parser.AllowUnquotedKeys(true);
  parser.AllowLinebreakInStrings(true);
  parser.EnableQuotingInvalidTokens(true);
  std::vector<std::pair<std::string, std::string>> values;
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        values.push_back({std::string(path), std::string(json)});
      });

  EXPECT_THAT(parser.AddChunk("{a: 'foo\nbar', b: baz}"), IsOk());
  EXPECT_THAT(parser.Finalize(), IsOk());

  EXPECT_THAT(values,
              ElementsAre(Pair("$.a", "\"foo\\nbar\""), Pair("$.b", "\"baz\""),
                          Pair("$", R"({"a": "foo\nbar", "b": "baz"})")));
}

TEST(JsonStreamParserTest, ConsumeChunkWithQuoting) {
  JsonStreamParser parser(JsonStreamParser::SeparatorPolicy::REMOVE);
  parser.EnableQuotingInvalidTokens(true);
  std::string filter = "$.a[*]";
  bool streaming = false;
  std::vector<std::string> output;

  auto receive_chunk = [&]() {
    absl::string_view s = parser.ConsumeChunk();
    if (!s.empty()) {
      output.emplace_back(s);
    }
  };
  parser.SetValueBeginCallback(
      [&](absl::string_view path, JsonStreamParser::ValueType type) {
        if (path == filter) {
          parser.ConsumeChunk();
          streaming = true;
        }
      });
  parser.SetValueEndCallback(
      [&](absl::string_view path, absl::string_view json) {
        if (path == filter && streaming) {
          receive_chunk();
          output.push_back("\n");
          streaming = false;
        }
      });

  std::string json = R"({"a": [true, 123, "abc", foo_bar]})";
  for (int i = 0; i < json.size(); i += 2) {
    EXPECT_THAT(parser.AddChunk(json.substr(i, 2)), IsOk());
    if (streaming) {
      receive_chunk();
    }
  }
  EXPECT_THAT(parser.Finalize(), IsOk());

  // Note: true/123/foo_bar come in one piece because with
  // EnableQuotingInvalidTokens it requires validation.
  // For "abc" we can use streaming.
  EXPECT_THAT(output, ElementsAre("true", "\n",             //
                                  "123", "\n",              //
                                  "\"a", "bc", "\"", "\n",  //
                                  "\"foo_bar\"", "\n"));
}

}  // namespace
}  // namespace koladata::functor::parallel
