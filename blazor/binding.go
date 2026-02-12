package blazor

import (
	"fmt"
	"strings"

	"github.com/a-h/templ"
)

// Field는 특정 입력 요소의 ID와 Name을 관리합니다.
type Field struct {
	ID   string
	Name string
}

// Attrs는 templ에서 <input { field.Attrs()... } /> 형태로 쓸 수 있게 해줍니다.
func (f Field) Attrs() templ.Attributes {
	attrs := templ.Attributes{"id": f.ID}
	if len(f.Name) != 0 {
		attrs["name"] = f.Name
	}
	return attrs
}

// Selector는 CSS 선택자(#id)를 반환합니다.
func (f Field) Selector() string {
	return "#" + f.ID
}

// Binding은 특정 영역(네임스페이스) 내의 필드들을 관리합니다.
type Binding struct {
	prefix string
}

func NewBinding(prefix string) *Binding {
	return &Binding{prefix: prefix}
}

func (b *Binding) Field(name string) Field {
	id := fmt.Sprintf("%s_%s", b.prefix, name)
	return Field{ID: id, Name: id}
}

func (b *Binding) ID(name string) Field {
	id := fmt.Sprintf("%s_%s", b.prefix, name)
	return Field{ID: id, Name: ""}
}

// HTMX 관련 속성을 빌드하는 도우미
type HXAttr struct {
	attrs templ.Attributes
}

func HX() *HXAttr {
	return &HXAttr{attrs: make(templ.Attributes)}
}

func (h *HXAttr) Post(url string) *HXAttr {
	h.attrs["hx-post"] = url
	return h
}

func (h *HXAttr) Target(selector string) *HXAttr {
	h.attrs["hx-target"] = selector
	return h
}

func (h *HXAttr) Include(selectors ...string) *HXAttr {
	h.attrs["hx-include"] = strings.Join(selectors, ", ")
	return h
}

func (h *HXAttr) Build() templ.Attributes {
	return h.attrs
}
