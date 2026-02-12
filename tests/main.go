package main

import (
	"log"

	"github.com/a-h/templ"
	"github.com/gofiber/fiber/v3"
	"github.com/snowmerak/fiber-blazor/blazor"
)

//blazor:bind
type CalcRequest struct {
	A int `form:"calc_a"`
	B int `form:"calc_b"`
}

type CalcData struct {
	Sum int
}

func main() {
	app := fiber.New()

	blazor.Static(app, "/statics", "./statics")

	app.Get("/", blazor.InitRender(Calculator(CalcData{}), "en", "Fiber Blazor Calculator"))

	app.Post("/calculate", blazor.SetRenderer(
		func(data *CalcData) templ.Component {
			return Result(*data)
		},
		func(req *BindedCalcRequest) (*CalcData, error) {
			return &CalcData{Sum: req.A + req.B}, nil
		},
	))

	log.Fatal(app.Listen(":3000"))
}
