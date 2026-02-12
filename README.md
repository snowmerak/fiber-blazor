# Fiber-Blazor

Fiber-Blazor is a web development framework for Go that brings the component-based, isolated state management philosophy of Blazor to the Go ecosystem, powered by **Fiber v3**, **Templ**, and **HTMX**.

## Key Concepts

- **Component Isolation**: Every component has its own isolated scope for form fields and IDs, preventing collisions even when the same component is rendered multiple times on a page.
- **Build-time Binding**: Uses a custom AST-based generator to append random suffixes to struct tags (like `form:"..."`) and generate type-safe binders.
- **Minimal JavaScript**: Leverages HTMX for server-side interactivity, drastically reducing the need for client-side JS.
- **Full Type Safety**: Type-safe request payloads and component properties using Go and Templ.
- **Embedded Templ Generation**: No need to install the `templ` binary separately; the generator runs it internally using the library.

## Project Structure

- `cmd/flazor/main.go`: The unified build tool. It performs code generation for binders and executes `templ generate`.
- `blazor/`: Core library providing the binding system, HTMX attribute builders, and generic renderers.
- `tests/`: A comprehensive example application (Calculator) demonstrating the end-to-end workflow.

## Getting Started

### Prerequisites

- **Go 1.25+**

### Installation

```bash
git clone https://github.com/snowmerak/fiber-blazor.git
cd fiber-blazor
go mod tidy
go install ./cmd/flazor/.
```

or

```bash
go install github.com/snowmerak/fiber-blazor/cmd/flazor@latest
```

## Workflow

### 1. Define a Bindable Struct
Annotate your struct with `//blazor:bind`. The generator will use your existing tags as a base.

```go
//blazor:bind
type CalcRequest struct {
    A int `form:"calc_a"`
    B int `form:"calc_b"`
}
```

### 2. Run the Generator
Run the generator from the root directory. It will scan your project, create `_gen.go` files with randomized tags, and generate Templ code.

```bash
flazor
```

### 3. Create your Component (.templ)
Use the generated `GetBindingOf[Struct]()` helper to automatically manage IDs and Names.

```templ
{{ binder := GetBindingOfCalcRequest() }}
{{ result := binder.ID("result") }}
<div id={ binder.ID("wrapper").ID }>
	<input type="number" { binder.A.Attrs()... } />
	<input type="number" { binder.B.Attrs()... } />
	
	{{ // Use the functional builder for HTMX attributes }}
	<button { blazor.Post("/calculate").
				Target(result.Selector()).
				Include(binder.A.Selector(), binder.B.Selector()).
				Build()... }>
		Calculate
	</button>
</div>
<div { result.Attrs()... }></div>
```

### 4. Implement the Handler
Use `blazor.SetRenderer` to handle the HTMX request. It automatically binds the randomized form data to the `Binded` version of your struct.

```go
app.Post("/calculate", blazor.SetRenderer(
    func(data *CalcData) templ.Component {
        return Result(*data)
    },
    func(req *BindedCalcRequest) (*CalcData, error) {
        return &CalcData{Sum: req.A + req.B}, nil
    },
))
```

## Running the Test Application

```bash
# 1. Generate binders and templates
flazor

# 2. Run the test server
go run ./tests
```

Open [http://localhost:3000](http://localhost:3000) to see the isolated component binding in action.
