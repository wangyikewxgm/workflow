/*
Copyright 2022 The KubeVela Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sets

import (
	"fmt"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/parser"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
)

const (
	// TagPatchKey specify the primary key of the list items
	TagPatchKey = "patchKey"
	// TagPatchStrategy specify a strategy of the strategic merge patch
	TagPatchStrategy = "patchStrategy"

	// StrategyRetainKeys notes on the strategic merge patch using the retainKeys strategy
	StrategyRetainKeys = "retainKeys"
	// StrategyReplace notes on the strategic merge patch will allow replacing list
	StrategyReplace = "replace"
	// StrategyJSONPatch notes on the strategic merge patch will follow the RFC 6902 to run JsonPatch
	StrategyJSONPatch = "jsonPatch"
	// StrategyJSONMergePatch notes on the strategic merge patch will follow the RFC 7396 to run JsonMergePatch
	StrategyJSONMergePatch = "jsonMergePatch"
)

var (
	notFoundErr = errors.Errorf("not found")
)

// UnifyParams params for unify
type UnifyParams struct {
	PatchStrategy string
}

// UnifyOption defines the option for unify
type UnifyOption interface {
	ApplyToOption(params *UnifyParams)
}

// UnifyByJSONPatch unify by json patch following RFC 6902
type UnifyByJSONPatch struct{}

// ApplyToOption apply to option
func (op UnifyByJSONPatch) ApplyToOption(params *UnifyParams) {
	params.PatchStrategy = StrategyJSONPatch
}

// UnifyByJSONMergePatch unify by json patch following RFC 7396
type UnifyByJSONMergePatch struct{}

// ApplyToOption apply to option
func (op UnifyByJSONMergePatch) ApplyToOption(params *UnifyParams) {
	params.PatchStrategy = StrategyJSONMergePatch
}

func newUnifyParams(options ...UnifyOption) *UnifyParams {
	params := &UnifyParams{}
	for _, op := range options {
		op.ApplyToOption(params)
	}
	return params
}

// CreateUnifyOptionsForPatcher create unify options for patcher
func CreateUnifyOptionsForPatcher(patcher cue.Value) (options []UnifyOption) {
	if IsJSONPatch(patcher) {
		options = append(options, UnifyByJSONPatch{})
	} else if IsJSONMergePatch(patcher) {
		options = append(options, UnifyByJSONMergePatch{})
	}
	return
}

type interceptor func(baseNode ast.Node, patchNode ast.Node) error

func listMergeProcess(field *ast.Field, key string, baseList, patchList *ast.ListLit) {
	kmaps := map[string]ast.Expr{}
	nElts := []ast.Expr{}
	keys := strings.Split(key, ",")
	for _, key := range keys {
		foundPatch := false
		for i, elt := range patchList.Elts {
			if _, ok := elt.(*ast.Ellipsis); ok {
				continue
			}
			nodev, err := lookUp(elt, strings.Split(key, ".")...)
			if err != nil {
				continue
			}
			foundPatch = true
			blit, ok := nodev.(*ast.BasicLit)
			if !ok {
				return
			}
			kmaps[fmt.Sprintf(key, blit.Value)] = patchList.Elts[i]
		}
		if !foundPatch {
			if len(patchList.Elts) == 0 {
				continue
			}
			return
		}

		hasStrategyRetainKeys := isStrategyRetainKeys(field)

		for i, elt := range baseList.Elts {
			if _, ok := elt.(*ast.Ellipsis); ok {
				continue
			}

			nodev, err := lookUp(elt, strings.Split(key, ".")...)
			if err != nil {
				continue
			}
			blit, ok := nodev.(*ast.BasicLit)
			if !ok {
				return
			}

			k := fmt.Sprintf(key, blit.Value)
			if v, ok := kmaps[k]; ok {
				if hasStrategyRetainKeys {
					baseList.Elts[i] = ast.NewStruct()
				}
				nElts = append(nElts, v)
				delete(kmaps, k)
			} else {
				nElts = append(nElts, ast.NewStruct())
			}

		}
	}
	for _, elt := range patchList.Elts {
		for _, v := range kmaps {
			if elt == v {
				nElts = append(nElts, v)
				break
			}
		}
	}

	nElts = append(nElts, &ast.Ellipsis{})
	patchList.Elts = nElts
}

func strategyPatchHandle() interceptor {
	return func(baseNode ast.Node, patchNode ast.Node) error {
		walker := newWalker(func(node ast.Node, ctx walkCtx) {
			field, ok := node.(*ast.Field)
			if !ok {
				return
			}

			value := peelCloseExpr(field.Value)

			switch val := value.(type) {
			case *ast.ListLit:
				key := ctx.Tags()[TagPatchKey]
				patchStrategy := ""
				tags := findCommentTag(field.Comments())
				for tk, tv := range tags {
					if tk == TagPatchKey {
						key = tv
					}
					if tk == TagPatchStrategy {
						patchStrategy = tv
					}
				}

				paths := append(ctx.Pos(), LabelStr(field.Label))
				baseSubNode, err := lookUp(baseNode, paths...)
				if err != nil {
					if errors.Is(err, notFoundErr) {
						return
					}
					baseSubNode = ast.NewList()
				}
				baselist, ok := baseSubNode.(*ast.ListLit)
				if !ok {
					return
				}
				if patchStrategy == StrategyReplace {
					baselist.Elts = val.Elts
				} else if key != "" {
					listMergeProcess(field, key, baselist, val)
				}

			default:
				if !isStrategyRetainKeys(field) {
					return
				}

				srcNode, _ := lookUp(baseNode, ctx.Pos()...)
				if srcNode != nil {
					switch v := srcNode.(type) {
					case *ast.StructLit:
						for _, elt := range v.Elts {
							if fe, ok := elt.(*ast.Field); ok &&
								LabelStr(fe.Label) == LabelStr(field.Label) {
								fe.Value = field.Value
							}
						}
					case *ast.File: // For the top level element
						for _, decl := range v.Decls {
							if fe, ok := decl.(*ast.Field); ok &&
								LabelStr(fe.Label) == LabelStr(field.Label) {
								fe.Value = field.Value
							}
						}
					}
				}
			}
		})
		walker.walk(patchNode)
		return nil
	}
}

func isStrategyRetainKeys(node *ast.Field) bool {
	tags := findCommentTag(node.Comments())
	for tk, tv := range tags {
		if tk == TagPatchStrategy && tv == StrategyRetainKeys {
			return true
		}
	}
	return false
}

// IsJSONMergePatch check if patcher is json merge patch
func IsJSONMergePatch(patcher cue.Value) bool {
	tags := findCommentTag(patcher.Doc())
	return tags[TagPatchStrategy] == StrategyJSONMergePatch
}

// IsJSONPatch check if patcher is json patch
func IsJSONPatch(patcher cue.Value) bool {
	tags := findCommentTag(patcher.Doc())
	return tags[TagPatchStrategy] == StrategyJSONPatch
}

// StrategyUnify unify the objects by the strategy
func StrategyUnify(base, patch cue.Value, options ...UnifyOption) (ret cue.Value, err error) {
	params := newUnifyParams(options...)
	var patchOpts []interceptor
	if params.PatchStrategy == StrategyJSONMergePatch || params.PatchStrategy == StrategyJSONPatch {
		_, err := OpenBaiscLit(base)
		if err != nil {
			return base, err
		}
	} else {
		patchOpts = []interceptor{strategyPatchHandle()}
	}
	return strategyUnify(base, patch, params, patchOpts...)
}

// nolint:staticcheck
func strategyUnify(base cue.Value, patch cue.Value, params *UnifyParams, patchOpts ...interceptor) (val cue.Value, err error) {
	if params.PatchStrategy == StrategyJSONMergePatch {
		return jsonMergePatch(base, patch)
	} else if params.PatchStrategy == StrategyJSONPatch {
		return jsonPatch(base, patch.LookupPath(cue.ParsePath("operations")))
	}
	openBase, err := OpenListLit(base)
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to open list it for merge")
	}
	patchFile, err := ToFile(patch.Syntax(cue.Docs(true), cue.ResolveReferences(true)))
	if err != nil {
		return cue.Value{}, err
	}
	for _, option := range patchOpts {
		if err := option(openBase, patchFile); err != nil {
			return cue.Value{}, errors.WithMessage(err, "process patchOption")
		}
	}

	baseInst := cuecontext.New().BuildFile(openBase)
	patchInst := cuecontext.New().BuildFile(patchFile)

	// s, _ := ToString(patchInst)
	// fmt.Println("======patch", s)
	// s, _ = ToString(baseInst)
	// fmt.Println("======base", s)

	ret := baseInst.Unify(patchInst)

	_, err = toString(ret, removeTmpVar)
	if err != nil {
		return ret, errors.WithMessage(err, " format result toString")
	}

	if err := ret.Err(); err != nil {
		return ret, errors.WithMessage(err, "result check err")
	}

	if err := ret.Validate(cue.All()); err != nil {
		return ret, errors.WithMessage(err, "result validate")
	}

	return ret, nil
}

func findCommentTag(commentGroup []*ast.CommentGroup) map[string]string {
	marker := "+"
	kval := map[string]string{}
	for _, group := range commentGroup {
		for _, lineT := range group.List {
			line := lineT.Text
			line = strings.TrimPrefix(line, "//")
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			if !strings.HasPrefix(line, marker) {
				continue
			}
			kv := strings.SplitN(line[len(marker):], "=", 2)
			if len(kv) == 2 {
				val := strings.TrimSpace(kv[1])
				if len(strings.Fields(val)) > 1 {
					continue
				}
				kval[strings.TrimSpace(kv[0])] = val
			}
		}
	}
	return kval
}

func jsonMergePatch(base cue.Value, patch cue.Value) (cue.Value, error) {
	ctx := cuecontext.New()
	baseJSON, err := base.MarshalJSON()
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to marshal base value")
	}
	patchJSON, err := patch.MarshalJSON()
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to marshal patch value")
	}
	merged, err := jsonpatch.MergePatch(baseJSON, patchJSON)
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to merge base value and patch value by JsonMergePatch")
	}
	output, err := openJSON(string(merged))
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to parse open basic lit for merged result")
	}
	return ctx.BuildFile(output), nil
}

func jsonPatch(base cue.Value, patch cue.Value) (cue.Value, error) {
	ctx := cuecontext.New()
	baseJSON, err := base.MarshalJSON()
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to marshal base value")
	}
	patchJSON, err := patch.MarshalJSON()
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to marshal patch value")
	}
	decodedPatch, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to decode patch")
	}

	merged, err := decodedPatch.Apply(baseJSON)
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to apply json patch")
	}
	output, err := openJSON(string(merged))
	if err != nil {
		return cue.Value{}, errors.Wrapf(err, "failed to parse open basic lit for merged result")
	}
	return ctx.BuildFile(output), nil
}

func isEllipsis(elt ast.Node) bool {
	_, ok := elt.(*ast.Ellipsis)
	return ok
}

func openJSON(data string) (*ast.File, error) {
	f, err := parser.ParseFile("-", data, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	ast.Walk(f, func(node ast.Node) bool {
		field, ok := node.(*ast.Field)
		if ok {
			v := field.Value
			switch lit := v.(type) {
			case *ast.StructLit:
				if len(lit.Elts) == 0 || !isEllipsis(lit.Elts[len(lit.Elts)-1]) {
					lit.Elts = append(lit.Elts, &ast.Ellipsis{})
				}
			case *ast.ListLit:
				if len(lit.Elts) == 0 || !isEllipsis(lit.Elts[len(lit.Elts)-1]) {
					lit.Elts = append(lit.Elts, &ast.Ellipsis{})
				}
			}
		}
		return true
	}, nil)
	if len(f.Decls) > 0 {
		if emb, ok := f.Decls[0].(*ast.EmbedDecl); ok {
			if s, _ok := emb.Expr.(*ast.StructLit); _ok {
				f.Decls = s.Elts
			}
		}
	}
	return f, nil
}
