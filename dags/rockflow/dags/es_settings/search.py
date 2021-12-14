search_setting = {
    "settings": {
        "analysis": {
            "filter": {
                "my_truncate_filter": {
                    "length": "1",
                    "type": "truncate"
                },
                "auto_complete_filter": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 16
                },
                "full_pinyin_filter": {
                    "keep_first_letter": "false",
                    "keep_full_pinyin": "true",
                    "keep_none_chinese_in_first_letter": "false",
                    "type": "pinyin"
                },
                "prefix_pinyin_filter": {
                    "keep_first_letter": "true",
                    "keep_full_pinyin": "false",
                    "keep_none_chinese_in_first_letter": "false",
                    "type": "pinyin"
                }
            },
            "analyzer": {
                "one_ngram_search_analyzer": {
                    "filter": "lowercase",
                    "tokenizer": "one_ngram_tokenizer"
                },
                "full_pinyin_analyzer": {
                    "filter": [
                        "full_pinyin_filter",
                        "my_truncate_filter",
                        "auto_complete_filter"
                    ],
                    "tokenizer": "one_ngram_tokenizer"
                },
                "prefix_pinyin_analyzer": {
                    "filter": [
                        "full_pinyin_filter",
                        "my_truncate_filter",
                        "auto_complete_filter"
                    ],
                    "tokenizer": "one_ngram_tokenizer"
                }
            },
            "tokenizer": {
                "one_ngram_tokenizer": {
                    "type": "ngram",
                    "min_gram": 1,
                    "max_gram": 1,
                    "token_chars": [
                        "digit",
                        "letter"
                    ]
                }
            }
        }
    },
    "mappings": {
        "dynamic": "false",
        "_source": {
            "enabled": "true"
        },
        "properties": {
            "symbol": {
                "type": "text",
                "analyzer": "prefix_pinyin_analyzer",
                "search_analyzer": "one_ngram_search_analyzer"
            },
            "raw": {
                "type": "text",
                "analyzer": "prefix_pinyin_analyzer",
                "search_analyzer": "one_ngram_search_analyzer"
            },
            "name": {
                "type": "text",
                "analyzer": "prefix_pinyin_analyzer",
                "search_analyzer": "one_ngram_search_analyzer"
            },
            "name_en": {
                "type": "text",
                "analyzer": "prefix_pinyin_analyzer",
                "search_analyzer": "one_ngram_search_analyzer"
            },
            "name_zh": {
                "type": "text",
                "analyzer": "prefix_pinyin_analyzer",
                "search_analyzer": "one_ngram_search_analyzer"
            },
            "profile_en": {
                "type": "keyword"
            },
            "profile_zh": {
                "type": "keyword"
            },
            "market": {
                "type": "keyword"
            }
        }
    }
}
