// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pro_macro::FromInner;

#[test]
fn named_struct() {
    #[derive(FromInner)]
    struct NamedStruct {
        inner: usize,
    }

    // unref
    let mut a = NamedStruct { inner: 0 };
    assert_eq!(*a, 0);

    // into
    let b = Into::<NamedStruct>::into(0);
    assert_eq!(*a, *b);

    // value add assign
    a += 1;
    assert_eq!(*a, 1);

    // value add
    let mut a = a + 1;
    assert_eq!(*a, 2);

    // ref add
    let a_ref = &a + 1;
    assert_eq!(*a_ref, 3);

    // mut ref add
    {
        let a_mut_ref = &mut a;
        assert_eq!(*(a_mut_ref + 1), 3);
        assert_eq!(*a, 2);
    }

    // mut ref add assign
    {
        let mut va_mut_ref = &mut a;
        va_mut_ref += 1;
        assert_eq!(**va_mut_ref, 3);
        assert_eq!(*a, 3);
    }
}

#[test]
fn unnamed_struct() {
    #[derive(FromInner)]
    struct UnnamedStruct(usize);

    // unref
    let mut a = UnnamedStruct(0);
    assert_eq!(*a, 0);

    // into
    let b = Into::<UnnamedStruct>::into(0);
    assert_eq!(*a, *b);

    // value add assign
    a += 1;
    assert_eq!(*a, 1);

    // value add
    let mut a = a + 1;
    assert_eq!(*a, 2);

    // ref add
    let a_ref = &a + 1;
    assert_eq!(*a_ref, 3);

    // mut ref add
    {
        let a_mut_ref = &mut a;
        assert_eq!(*(a_mut_ref + 1), 3);
        assert_eq!(*a, 2);
    }

    // mut ref add assign
    {
        let mut va_mut_ref = &mut a;
        va_mut_ref += 1;
        assert_eq!(**va_mut_ref, 3);
        assert_eq!(*a, 3);
    }
}
