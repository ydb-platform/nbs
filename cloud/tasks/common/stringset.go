package common

type StringSet struct {
	vals map[string]struct{}
}

func (s *StringSet) Has(val string) bool {
	if s.vals != nil {
		_, ok := s.vals[val]
		return ok
	}

	return false
}

func (s *StringSet) Vals() map[string]struct{} {
	return s.vals
}

func (s *StringSet) Size() int {
	return len(s.vals)
}

func (s *StringSet) Add(val string) {
	if s.vals == nil {
		s.vals = make(map[string]struct{})
	}

	s.vals[val] = struct{}{}
}

func (s *StringSet) Remove(val string) {
	if s.vals != nil {
		delete(s.vals, val)
	}
}

func (s *StringSet) List() []string {
	if s.vals == nil {
		return []string{}
	}

	vals := make([]string, 0, len(s.vals))
	for v := range s.vals {
		vals = append(vals, v)
	}
	return vals
}

func (s *StringSet) DeepCopy() StringSet {
	return NewStringSet(s.List()...)
}

func (s *StringSet) Subtract(other StringSet) StringSet {
	result := NewStringSet()

	for v := range s.vals {
		if !other.Has(v) {
			result.Add(v)
		}
	}

	return result
}

func NewStringSet(vals ...string) StringSet {
	if len(vals) == 0 {
		return StringSet{}
	}

	set := StringSet{
		vals: make(map[string]struct{}),
	}
	for _, val := range vals {
		set.Add(val)
	}
	return set
}
