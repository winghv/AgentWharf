package static

import (
	"context"
	"crypto/subtle"
	"fmt"

	"github.com/winghv/agentwharf/auth"
)

type Token struct {
	Token   string
	Subject string
	Scopes  []auth.Scope
}

type Authenticator struct {
	tokens []Token
}

func New(tokens []Token) *Authenticator {
	copied := make([]Token, 0, len(tokens))
	for _, token := range tokens {
		if token.Token == "" {
			continue
		}
		copied = append(copied, Token{
			Token:   token.Token,
			Subject: token.Subject,
			Scopes:  cloneScopes(token.Scopes),
		})
	}
	return &Authenticator{tokens: copied}
}

func (a *Authenticator) Authenticate(_ context.Context, token string) (auth.Principal, error) {
	if token == "" {
		return auth.Principal{}, auth.ErrInvalidToken
	}

	for _, candidate := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(candidate.Token)) == 1 {
			return auth.Principal{
				Subject: candidate.Subject,
				Scopes:  cloneScopes(candidate.Scopes),
			}, nil
		}
	}
	return auth.Principal{}, fmt.Errorf("%w: token not configured", auth.ErrInvalidToken)
}

func (a *Authenticator) Authorize(_ context.Context, principal auth.Principal, scope auth.Scope) error {
	return auth.Authorize(principal, scope)
}

func cloneScopes(scopes []auth.Scope) []auth.Scope {
	if len(scopes) == 0 {
		return nil
	}
	copied := make([]auth.Scope, len(scopes))
	copy(copied, scopes)
	return copied
}
