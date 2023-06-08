import typing as tp

import pytest
import typeguard

from danio import dataclass


class A(dataclass.BaseData):
    B: tp.ClassVar[int] = 1

    x: int
    y: str | None
    z: int | None = 4


class A2(A):
    a: int = 3


class A3(A2):
    l: tp.List[int] = dataclass.field(default=list)


def test_init():
    A(1, "s")
    A(1, "s", z=5)
    # params error
    with pytest.raises(TypeError):
        A()
    with pytest.raises(TypeError):
        A(1, z=5)
    with pytest.raises(TypeError):
        A(1, "s", 3, z=5)
    with pytest.raises(TypeError):
        A(1, "s", z=5, z2=4)
    with pytest.raises(TypeError):
        A(1, "s", z2=4)
    with pytest.raises(typeguard.TypeCheckError):
        A(1, 1)


def test_function():
    a = A(1, "s")
    for k, v in a.to_dict().items():
        assert v == getattr(a, k)
    a.to_json()


def test_inherence():
    A2(1, "2")
    a2 = A2(1, "2", 3, 4)
    assert a2.z == 3
    assert a2.a == 4
    a2 = A2(x=1, y="2", z=4)
    assert a2.a == 3

    # __match_args__
    matched = False
    match A2(1, "2"):
        case A2(1, "1"):
            pass
        case A2(1, "2", a=3):
            matched = True
        case _:
            pass
    assert matched == True


def test_with_field():
    a = A3(1, "2")
