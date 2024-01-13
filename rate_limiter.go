package scheduler

import (
	"context"
	"golang.org/x/time/rate"
)

type LimiterOptional = func(*rate.Limiter)

func WithLimiterTokenRate(rt float64) LimiterOptional {
	return func(limiter *rate.Limiter) {
		limiter.SetLimit(rate.Limit(rt))
	}
}

func WithLimiterCapacity(cp int) LimiterOptional {
	return func(limiter *rate.Limiter) {
		limiter.SetBurst(cp)
	}
}

type LimiterOptionals struct {
	Optionals []LimiterOptional
}

type RateLimiter struct {
	*rate.Limiter
}

func NewRateLimiter(optionals ...LimiterOptional) *RateLimiter {
	limiter := rate.NewLimiter(1, 1)
	for _, optional := range optionals {
		optional(limiter)
	}
	return &RateLimiter{
		limiter,
	}
}

type LimiterParams struct {
	Ctx       context.Context
	TokenRate float64
	Capacity  int
}

func (this LimiterParams) IsValid() bool {
	return this.Ctx != nil && this.TokenRate >= 0.0 && this.Capacity >= 0
}
