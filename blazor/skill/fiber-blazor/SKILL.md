---
name: fiber-blazor
description: Agent skill for fiber-blazor, a web application built with Fiber-Blazor framework.
metadata:
  module: github.com/snowmerak/fiber-blazor
  framework: fiber-blazor
---

# fiber-blazor Skill

This skill provides information on how to interact with the Fiber-Blazor components in this project.

## Key Concepts

- **Form Binding**: Look for `//blazor:bind` on structs. These generate randomized tags for security and isolation.
- **Templ Components**: Use `GetBindingOf[StructName]()` to get a binder that helps generate IDs and Names for HTML elements.
- **HTMX Attributes**: Use `blazor.Post()`, `blazor.Target()`, etc., to build htmx attributes in Go/Templ.

## Common Tasks

1. **Add a new bindable struct**: Add `//blazor:bind` above your struct definition.
2. **Generate code**: Run `flazor` to sync everything.
3. **Create a template**: Use the binder in your `.templ` file to bind inputs.
4. **Handle requests**: Use `blazor.SetRenderer` in your Fiber app to process the form data.
