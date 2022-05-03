from __future__ import annotations
from typing import Any, Optional, Dict, List, Tuple, Union, Callable

from copy import deepcopy
from abc import ABC, abstractmethod
from functools import wraps
import inspect
import logging

from haystack.schema import Document, MultiLabel
from haystack.errors import PipelineSchemaError
from haystack.telemetry import send_custom_event


logger = logging.getLogger(__name__)


def exportable_to_yaml(init_func):
    """
    Decorator that saves the init parameters of a node that later can
    be used with exporting YAML configuration of a Pipeline.
    """

    @wraps(init_func)
    def wrapper_exportable_to_yaml(self, *args, **kwargs):

        # Call the actuall __init__ function with all the arguments
        init_func(self, *args, **kwargs)

        # Create the configuration dictionary if it doesn't exist yet
        if not self._component_config:
            self._component_config = {"params": {}, "type": type(self).__name__}

        # Make sure it runs only on the __init__of the implementations, not in superclasses
        # NOTE: we use '.endswith' because inner classes's __qualname__ will include the parent class'
        #   name, like: ParentClass.InnerClass.__init__.
        #   Inner classes are heavily used in tests.
        if init_func.__qualname__.endswith(f"{self.__class__.__name__}.{init_func.__name__}"):

            # Store all the named input parameters in self._component_config
            for k, v in kwargs.items():
                self._component_config["params"][k] = v

            # Store unnamed input parameters in self._component_config too by inferring their names
            sig = inspect.signature(init_func)
            parameter_names = list(sig.parameters.keys())
            # we can be sure that the first one is always "self"
            arg_names = parameter_names[1 : 1 + len(args)]
            for arg, arg_name in zip(args, arg_names):
                self._component_config["params"][arg_name] = arg

    return wrapper_exportable_to_yaml


class BaseComponent(ABC):
    """
    A base class for implementing nodes in a Pipeline.
    """

    outgoing_edges: int
    _subclasses: dict = {}
    _component_config: dict = {}

    def __init__(self):
        # a small subset of the component's parameters is sent in an event after applying filters defined in haystack.telemetry.NonPrivateParameters
        send_custom_event(event=f"{type(self).__name__} initialized", payload=self._component_config.get("params", {}))

    # __init_subclass__ is invoked when a subclass of BaseComponent is _imported_
    # (not instantiated). It works approximately as a metaclass.
    def __init_subclass__(cls, **kwargs):

        super().__init_subclass__(**kwargs)

        # Automatically registers all the init parameters in
        # an instance attribute called `_component_config`,
        # used to save this component to YAML. See exportable_to_yaml()
        cls.__init__ = exportable_to_yaml(cls.__init__)

        # Keeps track of all available subclasses by name.
        # Enables generic load() for all specific component implementations.
        # Registers abstract classes and base classes too.
        cls._subclasses[cls.__name__] = cls

    @property
    def name(self) -> Optional[str]:
        return self._component_config.get("name", None)

    @name.setter
    def name(self, value: str):
        self._component_config["name"] = value

    @property
    def utilized_components(self) -> List[BaseComponent]:
        if "params" not in self._component_config:
            return list()
        return [param for param in self._component_config["params"].values() if isinstance(param, BaseComponent)]

    @property
    def type(self) -> str:
        return self._component_config["type"]

    def get_params(self, return_defaults: bool = False) -> Dict[str, Any]:
        component_signature = self._get_signature()
        params: Dict[str, Any] = {}
        for key, value in self._component_config["params"].items():
            if value != component_signature[key].default or return_defaults:
                params[key] = value
        if return_defaults:
            for key, param in component_signature.items():
                if key not in params:
                    params[key] = param.default
        return params

    @classmethod
    def get_subclass(cls, component_type: str):
        if component_type not in cls._subclasses.keys():
            raise PipelineSchemaError(f"Haystack component with the name '{component_type}' not found.")
        subclass = cls._subclasses[component_type]
        return subclass

    @classmethod
    def load_from_args(cls, component_type: str, **kwargs):
        """
        Load a component instance of the given type using the kwargs.

        :param component_type: name of the component class to load.
        :param kwargs: parameters to pass to the __init__() for the component.
        """
        subclass = cls.get_subclass(component_type)
        instance = subclass(**kwargs)
        return instance

    @classmethod
    def load_from_pipeline_config(cls, pipeline_config: dict, component_name: str):
        """
        Load an individual component from a YAML config for Pipelines.

        :param pipeline_config: the Pipelines YAML config parsed as a dict.
        :param component_name: the name of the component to load.
        """
        if pipeline_config:
            all_component_configs = pipeline_config["components"]
            all_component_names = [comp["name"] for comp in all_component_configs]
            component_config = next(comp for comp in all_component_configs if comp["name"] == component_name)
            component_params = component_config["params"]

            for key, value in component_params.items():
                if value in all_component_names:  # check if the param value is a reference to another component
                    component_params[key] = cls.load_from_pipeline_config(pipeline_config, value)

            component_instance = cls.load_from_args(component_config["type"], **component_params)
        else:
            component_instance = cls.load_from_args(component_name)
        return component_instance

    @abstractmethod
    def run(
        self,
        query: Optional[str] = None,
        file_paths: Optional[List[str]] = None,
        labels: Optional[MultiLabel] = None,
        documents: Optional[List[Document]] = None,
        meta: Optional[dict] = None,
    ) -> Tuple[Dict, str]:
        """
        Method that will be executed when the node in the graph is called.

        The argument that are passed can vary between different types of nodes
        (e.g. retriever nodes expect different args than a reader node)


        See an example for an implementation in haystack/reader/base/BaseReader.py
        :return:
        """
        pass

    @abstractmethod
    def run_batch(
        self,
        queries: Optional[Union[str, List[str]]] = None,
        file_paths: Optional[List[str]] = None,
        labels: Optional[Union[MultiLabel, List[MultiLabel]]] = None,
        documents: Optional[Union[List[Document], List[List[Document]]]] = None,
        meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        params: Optional[dict] = None,
        debug: Optional[bool] = None,
    ):
        pass

    def _dispatch_run(self, **kwargs) -> Tuple[Dict, str]:
        """
        The Pipelines call this method when run() is executed. This method in turn executes the _dispatch_run_general()
        method with the correct run method.
        """
        return self._dispatch_run_general(self.run, **kwargs)

    def _dispatch_run_batch(self, **kwargs):
        """
        The Pipelines call this method when run_batch() is executed. This method in turn executes the
        _dispatch_run_general() method with the correct run method.
        """
        return self._dispatch_run_general(self.run_batch, **kwargs)

    def _dispatch_run_general(self, run_method: Callable, **kwargs):
        """
        This method takes care of the following:
          - inspect run_method's signature to validate if all necessary arguments are available
          - pop `debug` and sets them on the instance to control debug output
          - call run_method with the corresponding arguments and gather output
          - collate `_debug` information if present
          - merge component output with the preceding output and pass it on to the subsequent Component in the Pipeline
        """
        arguments = deepcopy(kwargs)
        params = arguments.get("params") or {}

        run_signature_args = inspect.signature(run_method).parameters.keys()

        run_params: Dict[str, Any] = {}
        for key, value in params.items():
            if key == self.name:  # targeted params for this node
                if isinstance(value, dict):
                    # Extract debug attributes
                    if "debug" in value.keys():
                        self.debug = value.pop("debug")

                    for _k, _v in value.items():
                        if _k not in run_signature_args:
                            raise Exception(f"Invalid parameter '{_k}' for the node '{self.name}'.")

                run_params.update(**value)
            elif key in run_signature_args:  # global params
                run_params[key] = value

        run_inputs = {}
        for key, value in arguments.items():
            if key in run_signature_args:
                run_inputs[key] = value

        output, stream = run_method(**run_inputs, **run_params)

        # Collect debug information
        debug_info = {}
        if getattr(self, "debug", None):
            # Include input
            debug_info["input"] = {**run_inputs, **run_params}
            debug_info["input"]["debug"] = self.debug
            # Include output, exclude _debug to avoid recursion
            filtered_output = {key: value for key, value in output.items() if key != "_debug"}
            debug_info["output"] = filtered_output
        # Include custom debug info
        custom_debug = output.get("_debug", {})
        if custom_debug:
            debug_info["runtime"] = custom_debug

        # append _debug information from nodes
        all_debug = arguments.get("_debug", {})
        if debug_info:
            all_debug[self.name] = debug_info
        if all_debug:
            output["_debug"] = all_debug

        # add "extra" args that were not used by the node
        for k, v in arguments.items():
            if k not in output.keys():
                output[k] = v

        output["params"] = params
        return output, stream

    @classmethod
    def _get_signature(cls) -> Dict[str, inspect.Parameter]:
        component_classes = inspect.getmro(cls)
        component_signature: Dict[str, inspect.Parameter] = {
            param_key: parameter
            for class_ in component_classes
            for param_key, parameter in inspect.signature(class_).parameters.items()
        }
        return component_signature
