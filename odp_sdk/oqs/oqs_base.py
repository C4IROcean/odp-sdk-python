import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Tuple, Type, Union

from pydantic import BaseModel, validator

from odp_sdk.exc import OqsEvaluationError

if TYPE_CHECKING:
    from pydantic.typing import DictStrAny


class OqsEngine(BaseModel):
    predicates: Dict[str, Type["OqsBasePredicate"]]

    predicate_prefix: str = "#"
    reference_prefix: str = "$"

    def interpret(self, dct: Dict[str, Any]) -> "OqsBasePredicate":
        assert len(dct) == 1, "Predicate must be a single key-value pair"

        key = next(iter(dct))
        value = dct[key]

        if key.startswith(self.predicate_prefix):
            predicate_cls: Type[OqsBasePredicate] = self.predicates.get(key[1:])

            if predicate_cls is None:
                raise ValueError(f"Unknown predicate: {key[1:]}")

            elif not isinstance(value, (list, dict)):
                value = [value]

            return predicate_cls._interpret_args(*value, engine=self)

    def with_predicates(self, predicates: Dict[str, Type["OqsBasePredicate"]]) -> "OqsEngine":
        """Return a copy of the engine with the given predicates registered.

        Args:
            predicates: A dict of predicate names and classes to register alongside the engine's existing predicates.

        Returns:
            A copy of the engine with the given predicates registered.
        """
        return self.copy(update={"predicates": predicates})


class OqsUndefined(BaseModel):
    """Undefined value"""


class OqsBasePredicate(ABC, BaseModel):
    @classmethod
    def get_name(cls) -> str:
        return getattr(cls, "_op")

    @property
    def operator_key(self) -> str:
        return "#" + self.get_name()

    def evaluate(self, context: Mapping[str, Any]) -> Any:
        """Evaluate predicate

        TODO: Implement caching of evaluated values

        Returns:
            The evaluated value
        """
        try:
            return self._evaluate(context)
        except OqsEvaluationError as e:
            return e.fallback_value

    def pre_evaluate(self, context: Mapping[str, Any]) -> Any:
        """Regenerate the predicate tree with the given context

        Returns:
            New predicate tree where all references are replaced with their values
        """

        new_predicates = []
        has_unevaluated = False
        for child in self.get_children():
            eval = child.pre_evaluate(context)
            if isinstance(eval, OqsUndefined):
                has_unevaluated = True
                new_predicates.append(child)
            elif isinstance(eval, OqsBasePredicate):
                has_unevaluated = True
                new_predicates.append(eval)
            else:
                new_predicates.append(SingletonPredicate(value=eval))

        obj = self.with_predicates(new_predicates)
        if has_unevaluated:
            # If any of the children were undefined, the parent must be undefined too
            return obj

        # If none of the children were undefined, we can evaluate the parent and return the result as a singleton
        val = obj.evaluate({})
        return SingletonPredicate(value=val)

    @abstractmethod
    def get_children(self) -> Iterable["OqsBasePredicate"]:
        """Get children of the predicate

        Returns:
            A list of children
        """

    @abstractmethod
    def with_predicates(self, predicates: List["OqsBasePredicate"]) -> "OqsBasePredicate":
        """Return a copy of the predicate with the given predicates registered.

        Args:
            predicates: A dict of predicate names and classes to register alongside the engine's existing predicates.

        Returns:
            A copy of the predicate with the given predicates registered.
        """

    def walk(
        self,
        chain: Optional[List["OqsBasePredicate"]] = None,
    ) -> Iterable[Tuple["OqsBasePredicate", List["OqsBasePredicate"]]]:
        """Walk the predicate tree

        Returns:
            A tuple of current predicate and parent
        """
        chain = chain or []
        chain += [self]

        yield self, chain
        for child in self.get_children():
            yield from child.walk(chain=chain)

    @abstractmethod
    def _evaluate(self, context: Mapping[str, Any]) -> Any:
        """Evaluate predicate

        Returns:
            The evaluated value
        """

    @classmethod
    def _interpret_args(cls, *args: Any, engine: OqsEngine) -> "OqsBasePredicate":
        args = list(args)

        for i, arg in enumerate(args):
            if isinstance(arg, dict):
                if len(arg) == 1 and next(iter(arg)).startswith(engine.predicate_prefix):
                    args[i] = engine.interpret(arg)
                else:
                    args[i] = SingletonPredicate(value=arg)
            elif isinstance(arg, list):
                args[i] = ListPredicate._interpret_args(*arg, engine=engine)
            elif isinstance(arg, str) and arg.startswith(engine.reference_prefix):
                args[i] = ReferredSingletonPredicate(name=arg[1:])
            else:
                args[i] = SingletonPredicate(value=arg)

        return cls(*args)

    def dict(self, *_args, **_kwargs) -> "DictStrAny":
        return {self.operator_key: [child.dict() for child in self.get_children()]}


class OqsLeafPredicate(OqsBasePredicate, ABC):
    def get_children(self) -> Iterable["OqsBasePredicate"]:
        return []

    def pre_evaluate(self, context: Mapping[str, Any]) -> Any:
        return self.evaluate(context)

    def with_predicates(self, predicates: List["OqsBasePredicate"]) -> "OqsBasePredicate":
        raise RuntimeError("Leaf predicates cannot have children")


class SingletonPredicate(OqsLeafPredicate):
    _op = "CONSTANT"

    value: Any

    def __init__(self, value: Any, **kwargs):
        if isinstance(value, SingletonPredicate):
            value = value.value
        super().__init__(value=value, **kwargs)

    @validator("value")
    def _ensure_not_predicate(cls, value: Any) -> Any:
        if isinstance(value, SingletonPredicate):
            value = value.value
        elif isinstance(value, OqsBasePredicate):
            raise ValueError("SingletonPredicate cannot contain another predicate")
        return value

    def _evaluate(self, context: Mapping[str, Any]) -> Any:
        return self.value

    @classmethod
    def _interpret_args(cls, *args: Any, engine: OqsEngine) -> "OqsBasePredicate":
        args = list(args)
        if len(args) == 1:
            return cls(args[0])
        return cls(args)

    def dict(self, *_, **kwargs) -> "DictStrAny":
        dct = json.loads(BaseModel.json(self, **kwargs))
        val = dct["value"]
        if isinstance(val, dict):
            val = [val]
        return {self.operator_key: val}

    def primitive_value(self) -> Any:
        dct = json.loads(BaseModel.json(self))
        return dct["value"]


class OqsContextualPredicate(OqsLeafPredicate, ABC):
    name: str

    def pre_evaluate(self, context: Mapping[str, Any]) -> Any:
        return self._evaluate(context)

    def evaluate(self, context: Mapping[str, Any]) -> Any:
        val = self._evaluate(context)
        if isinstance(val, OqsUndefined):
            return None
        return val


class ReferredSingletonPredicate(OqsContextualPredicate):
    _op = "REF"

    def __init__(self, name: Union[str, SingletonPredicate], **kwargs):
        if isinstance(name, SingletonPredicate):
            name = name.value
        super().__init__(name=name, **kwargs)

    def _evaluate(self, context: Mapping[str, Any]) -> Any:
        return context.get(self.name, OqsUndefined())

    def dict(self, *_args, **_kwargs) -> "DictStrAny":
        return {self.operator_key: self.name}


class KeyedReferredSingletonPredicate(OqsContextualPredicate):
    _op = "KEYREF"

    key: Union[int, str]

    def __init__(self, name: str, key: Union[int, str], **kwargs):
        if isinstance(name, SingletonPredicate):
            name = name.value
        elif isinstance(name, ReferredSingletonPredicate):
            name = name.name
        if isinstance(key, SingletonPredicate):
            key = key.value
        elif isinstance(key, OqsBasePredicate):
            raise ValueError("Key must be a singleton")
        super().__init__(name=name, key=key, **kwargs)

    def _evaluate(self, context: Mapping[str, Any]) -> Any:
        base = context.get(self.name, OqsUndefined())
        if isinstance(base, OqsUndefined):
            return base
        try:
            return base[self.key]
        except (KeyError, IndexError, TypeError):
            return OqsUndefined()

    def dict(self, *_args, **_kwargs) -> "DictStrAny":
        return {self.operator_key: [self.name, self.key]}


class UnaryPredicate(OqsBasePredicate, ABC):
    predicate: OqsBasePredicate

    def __init__(self, predicate: OqsBasePredicate, **kwargs):
        super().__init__(predicate=predicate, **kwargs)

    def get_children(self) -> Iterable[OqsBasePredicate]:
        yield self.predicate

    def with_predicates(self, predicates: List["OqsBasePredicate"]) -> "OqsBasePredicate":
        if len(predicates) != 1:
            raise RuntimeError("Unary predicates must have exactly one child")
        return self.__class__(predicates[0])


class BinaryPredicate(OqsBasePredicate, ABC):
    """Base class for binary predicates

    A binary predicate is a predicate that takes two predicates as input.
    """

    left: OqsBasePredicate
    right: OqsBasePredicate

    def __init__(self, left: OqsBasePredicate, right: OqsBasePredicate, **kwargs):
        super().__init__(left=left, right=right, **kwargs)

    def get_children(self) -> Iterable[OqsBasePredicate]:
        yield self.left
        yield self.right

    def with_predicates(self, predicates: List["OqsBasePredicate"]) -> "OqsBasePredicate":
        if len(predicates) != 2:
            raise RuntimeError("Binary predicates must have exactly two children")
        return self.__class__(predicates[0], predicates[1])


class PredicateChain(OqsBasePredicate, ABC):
    """Base class for predicate chains

    A predicate chain is a predicate that takes a list of predicates as input.
    """

    predicates: List[OqsBasePredicate]

    def __init__(self, *predicates: OqsBasePredicate, **kwargs):
        super().__init__(predicates=list(predicates), **kwargs)

    def get_children(self) -> Iterable[OqsBasePredicate]:
        yield from self.predicates

    def with_predicates(self, predicates: List["OqsBasePredicate"]) -> "OqsBasePredicate":
        return self.__class__(*predicates)

    def pre_evaluate(self, context: Mapping[str, Any]) -> Any:
        evaluated = super(PredicateChain, self).pre_evaluate(context)
        if not isinstance(evaluated, type(self)):
            return evaluated

        if len(evaluated.predicates) == 1:
            return evaluated.predicates[0]

        preds = []
        for predicate in evaluated.predicates:
            if isinstance(predicate, type(self)):
                preds.extend(predicate.predicates)
            else:
                preds.append(predicate)
        return type(self)(*preds)


class ListPredicate(PredicateChain):
    _op = "LIST"

    def _evaluate(self, context: Mapping[str, Any]) -> Any:
        return [pred.evaluate(context) for pred in self.predicates]

    def pre_evaluate(self, context: Mapping[str, Any]) -> Any:
        # return [
        #     pred.value if isinstance(pred, SingletonPredicate)
        #     else pred.pre_evaluate(context)
        #     for pred in self.predicates
        # ]
        return [pred.pre_evaluate(context) for pred in self.predicates]
