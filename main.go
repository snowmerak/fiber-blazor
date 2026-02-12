package main

import (
	"log"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
	"github.com/snowmerak/fiber-blazor/blazor"
)

type CalcRequest struct {
	A int `form:"a"`
	B int `form:"b"`
}

type CalcData struct {
	Sum int
}

func main() {
	app := fiber.New()

	// Statics 핸들러 등록
	blazor.Static(app, "/statics", "./statics")

	cb := blazor.NewBinding()

	// Main page
	app.Get("/", blazor.InitRender(Calculator(cb), "en", "Fiber Blazor Calculator"))

	// HTMX calculation endpoint
	app.Post("/calculate", blazor.SetRenderer(
		func(data *CalcData) templ.Component {
			return Result(data.Sum)
		},
		func(req *CalcRequest) (*CalcData, error) {
			return &CalcData{Sum: req.A + req.B}, nil
		},
		cb,
	))

	log.Fatal(app.Listen(":3000"))
}
