package blugeindex

import (
	"unicode"

	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/token"
	"github.com/blugelabs/bluge/analysis/tokenizer"
)

func NewFilenameAnalyzer() *analysis.Analyzer {
	return &analysis.Analyzer{
		Tokenizer: NewFilenameTokenizer(),
		TokenFilters: []analysis.TokenFilter{
			token.NewLowerCaseFilter(),
		},
	}
}

func NewFilenameTokenizer() *tokenizer.CharacterTokenizer {
	return tokenizer.NewCharacterTokenizer(letterOrNumber)
}

func letterOrNumber(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}
