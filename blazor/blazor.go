package blazor

import (
	"log"
	"reflect"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/static"
)

const defaultTitle = "Fiber Blazor App"
const defaultLang = "en"

func InitRender(root templ.Component, lang string, title string) fiber.Handler {
	if title == "" {
		title = defaultTitle
	}
	if lang == "" {
		lang = defaultLang
	}
	return func(c fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return Page(title, lang, root).Render(c.Context(), c.Res().Response().BodyWriter())
	}
}

func Static(app *fiber.App, prefix, rootDir string) {
	app.Use(prefix, static.New(rootDir))
}

func SetContextRenderer[V any](componentFunc func(data *V) templ.Component, transform func(ctx fiber.Ctx) (*V, error)) fiber.Handler {
	return func(c fiber.Ctx) error {
		data, err := transform(c)
		if err != nil {
			return fiber.ErrBadRequest
		}
		component := componentFunc(data)
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return component.Render(c.Context(), c.Res().Response().BodyWriter())
	}
}

func SetRenderer[T, V any](componentFunc func(data *V) templ.Component, transform func(req *T) (*V, error), binding *Binding) fiber.Handler {
	return func(c fiber.Ctx) error {
		req := binding.Bind(new(T))
		if err := c.Bind().All(req); err != nil {
			return fiber.ErrBadRequest
		}
		log.Printf("Received request: %+v", req)
		originReq := new(T)
		reflect.ValueOf(originReq).Elem().Set(reflect.ValueOf(req).Elem())
		log.Printf("Transformed request: %+v", originReq)
		data, err := transform(originReq)
		if err != nil {
			return fiber.ErrBadRequest
		}

		component := componentFunc(data)

		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return component.Render(c.Context(), c.Res().Response().BodyWriter())
	}
}
