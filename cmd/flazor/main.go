package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/a-h/templ/cmd/templ/generatecmd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func randomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "static"
	}
	return hex.EncodeToString(b)
}

func run() error {
	// 1. Scan for //blazor:bind
	if err := generateBinders("."); err != nil {
		return fmt.Errorf("generate binders: %w", err)
	}

	// 2. Generate Agent Skill
	if err := generateSkill("."); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to generate skill: %v\n", err)
	}

	// 3. Run templ generate
	fmt.Println("Running templ generate...")
	ctx := context.Background()
	// Pass empty args logic or just run with defaults
	err := generatecmd.Run(ctx, os.Stdout, os.Stderr, nil)
	if err != nil {
		return fmt.Errorf("templ generate: %w", err)
	}

	return nil
}

func generateSkill(root string) error {
	modData, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return nil // Not a go module root
	}
	modLines := strings.Split(string(modData), "\n")
	var modName string
	for _, line := range modLines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			modName = strings.TrimPrefix(line, "module ")
			break
		}
	}

	parts := strings.Split(strings.TrimSpace(modName), "/")
	shortName := parts[len(parts)-1]
	skillName := strings.ToLower(shortName)
	// Validate skillName based on spec: Lowercase letters, numbers, and hyphens only.
	reg := regexp.MustCompile(`[^a-z0-9-]+`)
	skillName = reg.ReplaceAllString(skillName, "-")
	skillName = strings.Trim(skillName, "-")

	// The spec says skill directory must match the name.
	skillDir := filepath.Join(root, skillName)
	// Special case for this project: if blazor/skill exists, use it as a base
	blazorSkillDir := filepath.Join(root, "blazor", "skill")
	if _, err := os.Stat(blazorSkillDir); err == nil {
		skillDir = filepath.Join(blazorSkillDir, skillName)
	}

	if _, err := os.Stat(skillDir); os.IsNotExist(err) {
		if err := os.MkdirAll(skillDir, 0755); err != nil {
			return err
		}
	}

	skillPath := filepath.Join(skillDir, "SKILL.md")

	description := fmt.Sprintf("Agent skill for %s, a web application built with Fiber-Blazor framework.", shortName)

	var sb strings.Builder
	sb.WriteString("---\n")
	sb.WriteString(fmt.Sprintf("name: %s\n", skillName))
	sb.WriteString(fmt.Sprintf("description: %s\n", description))
	sb.WriteString("metadata:\n")
	sb.WriteString(fmt.Sprintf("  module: %s\n", modName))
	sb.WriteString("  framework: fiber-blazor\n")
	sb.WriteString("---\n\n")

	sb.WriteString(fmt.Sprintf("# %s Skill\n\n", shortName))
	sb.WriteString("This skill provides information on how to interact with the Fiber-Blazor components in this project.\n\n")

	sb.WriteString("## Key Concepts\n\n")
	sb.WriteString("- **Form Binding**: Look for `//blazor:bind` on structs. These generate randomized tags for security and isolation.\n")
	sb.WriteString("- **Templ Components**: Use `GetBindingOf[StructName]()` to get a binder that helps generate IDs and Names for HTML elements.\n")
	sb.WriteString("- **HTMX Attributes**: Use `blazor.Post()`, `blazor.Target()`, etc., to build htmx attributes in Go/Templ.\n\n")

	sb.WriteString("## Common Tasks\n\n")
	sb.WriteString("1. **Add a new bindable struct**: Add `//blazor:bind` above your struct definition.\n")
	sb.WriteString("2. **Generate code**: Run `flazor` to sync everything.\n")
	sb.WriteString("3. **Create a template**: Use the binder in your `.templ` file to bind inputs.\n")
	sb.WriteString("4. **Handle requests**: Use `blazor.SetRenderer` in your Fiber app to process the form data.\n")

	err = os.WriteFile(skillPath, []byte(sb.String()), 0644)
	if err != nil {
		return err
	}

	fmt.Printf("Generated Agent Skill at %s\n", skillPath)
	return nil
}

func generateBinders(root string) error {
	fset := token.NewFileSet()
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "vendor" || info.Name() == ".git" || info.Name() == "blazor" || info.Name() == "statics" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_gen.go") || path == "main.go" {
			return nil
		}

		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil // Skip files that don't parse
		}

		var typesToGen []string
		var structFields = make(map[string][]fieldInfo)

		for _, decl := range f.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok || genDecl.Tok != token.TYPE {
				continue
			}

			shouldGen := false
			if genDecl.Doc != nil {
				for _, comment := range genDecl.Doc.List {
					if strings.Contains(comment.Text, "//blazor:bind") {
						shouldGen = true
						break
					}
				}
			}

			if !shouldGen {
				continue
			}

			for _, spec := range genDecl.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				typeName := typeSpec.Name.Name
				typesToGen = append(typesToGen, typeName)

				var fields []fieldInfo
				for _, field := range structType.Fields.List {
					if len(field.Names) == 0 {
						continue
					}
					fieldName := field.Names[0].Name
					tag := ""
					if field.Tag != nil {
						tag = field.Tag.Value
					}

					// Get field type string
					var typeBuf strings.Builder
					format.Node(&typeBuf, fset, field.Type)
					fieldType := typeBuf.String()

					// Basic tag parsing for 'field' or 'form'
					bindName := strings.ToLower(fieldName)
					if strings.Contains(tag, `form:"`) {
						parts := strings.Split(tag, `form:"`)
						if len(parts) > 1 {
							bindName = strings.Split(parts[1], `"`)[0]
						}
					}

					fields = append(fields, fieldInfo{
						FieldName:   fieldName,
						BindName:    bindName,
						FieldType:   fieldType,
						OriginalTag: tag,
					})
				}
				structFields[typeName] = fields
			}
		}

		if len(typesToGen) > 0 {
			if err := writeGenFile(path, f.Name.Name, typesToGen, structFields); err != nil {
				return err
			}
		}

		return nil
	})
}

type fieldInfo struct {
	FieldName   string
	BindName    string
	FieldType   string
	OriginalTag string
}

func writeGenFile(srcPath, pkgName string, types []string, fields map[string][]fieldInfo) error {
	dir := filepath.Dir(srcPath)
	genPath := filepath.Join(dir, strings.TrimSuffix(filepath.Base(srcPath), ".go")+"_gen.go")

	f, err := os.Create(genPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "// Code generated by blazor-gen. DO NOT EDIT.\n")
	fmt.Fprintf(f, "package %s\n\n", pkgName)
	fmt.Fprintf(f, "import \"github.com/snowmerak/fiber-blazor/blazor\"\n\n")

	tagRegex := regexp.MustCompile(`(\w+):"([^"]*)"`)

	for _, t := range types {
		structSuffix := randomString(4)

		fmt.Fprintf(f, "type Binded%s struct {\n", t)
		for _, field := range fields[t] {
			newTag := field.OriginalTag
			if newTag != "" {
				// Remove the backticks if present to make regex matching easier, then put them back
				isBackticked := strings.HasPrefix(newTag, "`") && strings.HasSuffix(newTag, "`")
				tagContent := newTag
				if isBackticked {
					tagContent = newTag[1 : len(newTag)-1]
				}

				newTag = tagRegex.ReplaceAllString(tagContent, `${1}:"${2}_`+structSuffix+`"`)

				if isBackticked {
					newTag = "`" + newTag + "`"
				}
			}
			fmt.Fprintf(f, "\t%s %s %s\n", field.FieldName, field.FieldType, newTag)
		}
		fmt.Fprintf(f, "}\n\n")

		constPrefix := "bind_" + t + "_"
		fmt.Fprintf(f, "const (\n")
		for _, field := range fields[t] {
			fmt.Fprintf(f, "\t%s%s = \"%s_%s\"\n", constPrefix, field.FieldName, field.BindName, structSuffix)
		}
		fmt.Fprintf(f, ")\n\n")

		binderName := "BindingOf" + t
		fmt.Fprintf(f, "type %s struct {\n", binderName)
		fmt.Fprintf(f, "\t*blazor.Binding\n")
		for _, field := range fields[t] {
			fmt.Fprintf(f, "\t%s blazor.Field\n", field.FieldName)
		}
		fmt.Fprintf(f, "}\n\n")

		fmt.Fprintf(f, "func Get%s() %s {\n", binderName, binderName)
		fmt.Fprintf(f, "\tb := blazor.NewBinding()\n")
		fmt.Fprintf(f, "\treturn %s{\n", binderName)
		fmt.Fprintf(f, "\t\tBinding: b,\n")
		for _, field := range fields[t] {
			fmt.Fprintf(f, "\t\t%s: b.Field(%s%s),\n", field.FieldName, constPrefix, field.FieldName)
		}
		fmt.Fprintf(f, "\t}\n")
		fmt.Fprintf(f, "}\n\n")
	}

	fmt.Printf("Generated %s\n", genPath)
	return nil
}
