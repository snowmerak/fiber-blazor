package blazor

import (
	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/static"
)

const defaultTitle = "Fiber Blazor App"
const defaultLang = "en"

func InitRender(root templ.Component, lang string, title string) fiber.Handler {
	return func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return Page(title, lang, root).Render(c.Context(), c.Res().Response().BodyWriter())
	}
}

func Static(app *fiber.App, prefix, rootDir string) {
	app.Use(prefix, static.New(rootDir))
}

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
