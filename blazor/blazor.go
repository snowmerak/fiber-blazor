package blazor

import (
	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
)

func SetRenderer[T, V any](componentFunc func(data *V) templ.Component, transform func(req *T) (*V, error)) fiber.Handler {
	return func(c fiber.Ctx) error {
		req := new(T)
		if err := c.Bind().All(req); err != nil {
			return fiber.ErrBadRequest
		}
		data, err := transform(req)
		if err != nil {
			return fiber.ErrBadRequest
		}
		component := componentFunc(data)
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return component.Render(c.Context(), c.Res().Response().BodyWriter())
	}
}
