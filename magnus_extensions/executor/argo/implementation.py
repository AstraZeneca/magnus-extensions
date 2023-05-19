import json
import logging
import random
import shlex
import string
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Union

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph, create_node, search_node_by_internal_name
from magnus.nodes import BaseNode
from pydantic import BaseModel
from ruamel.yaml import YAML

logger = logging.getLogger(defaults.NAME)


class SecretEnvVar(BaseModel):
    """
    Renders:
    env:
      - name: MYSECRETPASSWORD
        valueFrom:
          secretKeyRef:
            name: my-secret
            key: mypassword
    """

    environment_variable: str
    secret_name: str
    secret_key: str

    def dict(self, *args, **kwargs):
        return {
            "name": self.environment_variable,
            "valueFrom": {
                "secretKeyRef": {"name": self.secret_name, "key": self.secret_key}
            },
        }


class EnvVar(BaseModel):
    """
    Renders:
    parameters: # in arguments
      - name: x
        value: 3 # This is optional for workflow parameters
    """

    name: str
    value: Any

    def dict(self, *args, **kwargs):
        return_value = {"name": self.name}

        if self.value:
            return_value["value"] = self.value

        return return_value


class Request(BaseModel):
    """
    Renders:
    requests:
      memory: 1G
      cpu: 250m
    """

    memory: str
    cpu: str


class Limit(BaseModel):
    """
    Renders:
    limits:
      cpu: 250m
      memory: 1G
      nvidia.com/gpu: 1 # If you want a GPU
    """

    memory: str
    cpu: str
    gpu: int = 0

    def dict(self, *args, **kwargs) -> dict:
        resource = {"cpu": self.cpu, "memory": self.memory}
        if self.gpu:
            # TODO: This should be via config to allow users to specify the vendor
            resource["nvidia.com/gpu"] = self.gpu

        return resource


class VolumeMount(BaseModel):
    """
    Renders: in volumeMounts in templateDefaults or container
      - mountPath: /mnt/
        name: executor-0
    """

    mountPath: str
    name: str


class TemplateDefaults(BaseModel):
    """
    Renders:
    templateDefaults: # In the spec
        limits:
        cpu: 250m
        memory: 1G
        requests:
        memory: 1G
        cpu: 250m
        imagePullPolicy: Always
        retry: 1
        volumeMounts:
        - mountPath: /mnt/
            name: executor-0
    """

    limits: Optional[Limit] = None
    requests: Optional[Request] = None
    imagePullPolicy: Optional[str] = "Always"
    retry: Optional[int] = None
    volumeMounts: List[VolumeMount] = []


class Toleration(BaseModel):
    effect: str
    key: str
    operator: str
    value: str


class Container(TemplateDefaults):
    """
    Renders:
    retry: 1
    image:
    command:
    env: []
    volumeMounts:
        - mountPath: /mnt/
        name: executor-0
    """

    command: List[str]
    image: str
    env: List[Union[SecretEnvVar, EnvVar]] = []
    nodeSelectors: Dict[str, str] = {}
    tolerations: Optional[List[Toleration]] = []

    def dict(self, *args, **kwargs) -> dict:
        return_value = {}

        objects = ["limits", "requests", "imagePullPolicy", "retry"]
        for obj in objects:
            if getattr(self, obj):
                return_value[obj] = getattr(self, obj)

        return_value["image"] = self.image
        return_value["command"] = self.command
        return_value["env"] = [e.dict() for e in self.env]
        return_value["volumeMounts"] = [volume.dict() for volume in self.volumeMounts]

        if self.nodeSelectors:
            return_value["nodeSelectors"] = self.nodeSelectors

        if self.tolerations:
            return_value["tolerations"] = [tol.dict() for tol in self.tolerations]

        return return_value


class Parameter(BaseModel):
    name: str
    value: Optional[str] = None

    def dict(self, *args, **kwargs) -> dict:
        rv = {}
        rv["name"] = self.name
        if self.value:
            rv["value"] = self.value

        return rv


class OutputParameter(Parameter):
    """
    Renders:
    - name: step-name
      valueFrom:
        path: /tmp/output.txt
    """

    path: str = "/tmp/output.txt"

    def dict(self, *args, **kwargs) -> dict:
        return {"name": self.name, "valueFrom": {"path": self.path}}


class Argument(BaseModel):
    """
    Templates are called with arguments, which become inputs for the template
    Renders:
    arguments:
      parameters:
        - name: The name of the parameter
          value: The value of the parameter
    """

    name: str
    value: str


class TaskTemplate(BaseModel):
    """
    dag:
        tasks:
          name: A
            template: nested-diamond
            arguments:
                parameters: [{name: message, value: A}]
    """

    name: str
    template: str
    depends: List[str] = []
    inputs: List[Parameter] = []
    arguments: List[Argument] = []
    with_param: Optional[str] = None  # If the

    def dict(self, *args, **kwargs) -> dict:
        rv = {
            "name": self.name,
            "template": self.template,
            "depends": " || ".join(self.depends),
        }
        if self.arguments:
            rv["arguments"] = {"parameters": [arg.dict() for arg in self.arguments]}

        if self.with_param:
            rv["withParam"] = self.with_param

        if self.inputs:
            rv["inputs"] = {"inputs": [param.dict() for param in self.inputs]}

        return rv


class ContainerTemplate(BaseModel):
    # These templates are used for actual execution nodes.
    name: str
    container: Container
    outputs: List[OutputParameter] = []
    inputs: List[Parameter] = []

    def dict(self, *args, **kwargs) -> dict:
        rv = {"name": self.name, "container": self.container.dict()}

        if self.outputs:
            rv["outputs"] = {"parameters": [param.dict() for param in self.outputs]}

        if self.inputs:
            rv["inputs"] = {"parameters": [param.dict() for param in self.inputs]}

        return rv

    def __hash__(self):
        return hash(self.name)


class DagTemplate(BaseModel):
    name: str = "magnus-dag"
    tasks: List[TaskTemplate] = []
    inputs: List[Parameter] = []

    def dict(self, *args, **kwargs):
        rv = {
            "name": self.name,
            "dag": {"tasks": [task.dict() for task in self.tasks]},
        }
        if self.inputs:
            rv["inputs"] = {"parameters": [param.dict() for param in self.inputs]}

        return rv


class Volume(BaseModel):
    name: str
    claim: str

    def dict(self, *args, **kwargs) -> dict:
        return {"name": self.name, "persistentVolumeClaim": {"claimName": self.claim}}


class Spec(BaseModel):
    entrypoint: str = "magnus-dag"
    templates: Optional[DagTemplate] = []
    serviceAccountName: str = "pipeline-runner"
    arguments: List[EnvVar] = []
    volumes: List[Volume] = []
    parallelism: int = 0
    templateDefaults: TemplateDefaults = None
    nodeSelector: dict = {}
    tolerations: List[Toleration] = []

    def dict(self, *args, **kwargs):
        return_value = {
            "entrypoint": self.entrypoint,
            "volumes": [v.dict() for v in self.volumes],
            "templates": [template.dict() for template in self.templates],
            "serviceAccountName": self.serviceAccountName,
            "arguments": {"parameters": [env.dict() for env in self.arguments]},
        }
        if self.parallelism:
            return_value["parallelism"] = self.parallelism

        if self.templateDefaults:
            return_value["templateDefaults"] = self.templateDefaults.dict()

        if self.nodeSelector:
            return_value["nodeSelector"] = self.nodeSelector

        if self.tolerations:
            return_value["tolerations"] = [tol.dict() for tol in self.tolerations]

        return return_value


class WorkSpec(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: dict = {"generateName": "magnus-dag-"}
    spec: Optional[Spec]


class NodeRenderer:
    allowed_node_types: List[str] = []

    def __init__(self, executor: BaseExecutor, node: BaseNode) -> None:
        self.executor = executor
        self.node = node

    def render(self, list_of_iter_values: List = None):
        pass


class ExecutionNode(NodeRenderer):
    allowed_node_types = ["task", "as-is", "success", "fail"]

    def render(self, list_of_iter_values: List = None):
        """
        Compose the map variable and create the execution command.
        Create an input to the command.
        create_container_template : creates an argument for the list of iter values
        """
        map_variable = self.executor.compose_map_variable(list_of_iter_values)
        command = utils.get_node_execution_command(
            self.executor,
            self.node,
            over_write_run_id=self.executor.run_id_placeholder,
            map_variable=map_variable,
        )

        inputs = []
        if list_of_iter_values:
            for val in list_of_iter_values:
                inputs.append(Parameter(name=val))

        # Create the container template
        container_template = self.executor.create_container_template(
            working_on=self.node,
            command=command,
            inputs=inputs,
        )

        self.executor.container_templates.append(container_template)


class DagNode(NodeRenderer):
    allowed_node_types = ["dag"]

    def render(self, list_of_iter_values: List = None):
        task_template_arguments = []
        dag_inputs = []
        if list_of_iter_values:
            for value in list_of_iter_values:
                task_template_arguments.append(
                    Parameter(name=value, value="{{inputs.parameters." + value + "}}")
                )
                dag_inputs.append(Parameter(name=value))

        clean_name = self.executor.get_clean_name(self.node)
        fan_out_template = self.executor._create_fan_out_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_out_template.arguments = task_template_arguments

        fan_in_template = self.executor._create_fan_in_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_in_template.arguments = task_template_arguments

        self.executor._gather_task_templates_of_dag(
            self.node.branch,
            dag_name=f"{clean_name}-branch",
            list_of_iter_values=list_of_iter_values,
        )

        branch_template = TaskTemplate(
            name=f"{clean_name}-branch",
            template=f"{clean_name}-branch",
            arguments=task_template_arguments,
        )
        branch_template.depends.append(f"{clean_name}-fan-out.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-branch.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-branch.Failed")

        self.executor.templates.append(
            DagTemplate(
                tasks=[fan_out_template, branch_template, fan_in_template],
                name=clean_name,
                inputs=dag_inputs,
            )
        )


class ParallelNode(NodeRenderer):
    allowed_node_types = ["parallel"]

    def render(self, list_of_iter_values: List = None):
        task_template_arguments = []
        dag_inputs = []
        if list_of_iter_values:
            for value in list_of_iter_values:
                task_template_arguments.append(
                    Parameter(name=value, value="{{inputs.parameters." + value + "}}")
                )
                dag_inputs.append(Parameter(name=value))

        clean_name = self.executor.get_clean_name(self.node)
        fan_out_template = self.executor._create_fan_out_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_out_template.arguments = task_template_arguments

        fan_in_template = self.executor._create_fan_in_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_in_template.arguments = task_template_arguments

        branch_templates = []
        for name, branch in self.node.branches.items():
            branch_name = self.executor.sanitize_name(name)
            self.executor._gather_task_templates_of_dag(
                branch,
                dag_name=f"{clean_name}-{branch_name}",
                list_of_iter_values=list_of_iter_values,
            )
            task_template = TaskTemplate(
                name=f"{clean_name}-{branch_name}",
                template=f"{clean_name}-{branch_name}",
                arguments=task_template_arguments,
            )
            task_template.depends.append(f"{clean_name}-fan-out.Succeeded")
            fan_in_template.depends.append(f"{task_template.name}.Succeeded")
            fan_in_template.depends.append(f"{task_template.name}.Failed")
            branch_templates.append(task_template)

        self.executor.templates.append(
            DagTemplate(
                tasks=[fan_out_template] + branch_templates + [fan_in_template],
                name=clean_name,
                inputs=dag_inputs,
            )
        )


class MapNode(NodeRenderer):
    allowed_node_types = ["map"]

    def render(self, list_of_iter_values: List = None):
        task_template_arguments = []
        dag_inputs = []
        if list_of_iter_values:
            for value in list_of_iter_values:
                task_template_arguments.append(
                    Parameter(name=value, value="{{inputs.parameters." + value + "}}")
                )
            dag_inputs.append(Parameter(name=value))

        clean_name = self.executor.get_clean_name(self.node)
        fan_out_template = self.executor._create_fan_out_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_out_template.arguments = task_template_arguments

        fan_in_template = self.executor._create_fan_in_template(
            composite_node=self.node, list_of_iter_values=list_of_iter_values
        )
        fan_in_template.arguments = task_template_arguments

        if not list_of_iter_values:
            list_of_iter_values = []

        list_of_iter_values.append(self.node.iterate_as)

        self.executor._gather_task_templates_of_dag(
            self.node.branch,
            dag_name=f"{clean_name}-map",
            list_of_iter_values=list_of_iter_values,
        )

        task_template = TaskTemplate(
            name=f"{clean_name}-map",
            template=f"{clean_name}-map",
            arguments=task_template_arguments,
        )
        task_template.with_param = (
            "{{tasks."
            + f"{clean_name}-fan-out"
            + ".outputs.parameters."
            + "iterate-on"
            + "}}"
        )

        argument = Argument(name=self.node.iterate_as, value="{{item}}")
        task_template.arguments.append(argument)

        task_template.depends.append(f"{clean_name}-fan-out.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-map.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-map.Failed")

        self.executor.templates.append(
            DagTemplate(
                tasks=[fan_out_template, task_template, fan_in_template],
                name=clean_name,
                inputs=dag_inputs,
            )
        )


def get_renderer(node):
    renderers = NodeRenderer.__subclasses__()

    for renderer in renderers:
        if node.node_type in renderer.allowed_node_types:
            return renderer
    raise Exception("This node type is not render-able")


class ArgoExecutor(BaseExecutor):
    service_name = "argo"
    run_id_placeholder = "{{workflow.parameters.run_id}}"

    class Config(BaseExecutor.Config):
        docker_image: str
        output_file: str = "argo-pipeline.yaml"
        cpu_limit: str = "250m"
        memory_limit: str = "1G"
        cpu_request: str = ""
        memory_request: str = ""
        gpu_limit: int = 0
        parallelism: int = 0
        enable_caching: bool = False
        image_pull_policy: str = "Always"
        secrets_from_k8s: dict = {}  # EnvVar=SecretName:Key
        # volume-name:mount_path, not available in executor_config
        persistent_volumes: dict = {}
        # TODO: Tolerations and node selectors should be added
        node_selectors: dict = {}
        tolerations: List[Toleration] = []

    def __init__(self, config: dict = None):
        super().__init__(config)

        self.persistent_volumes = {}
        self.volume_mounts: List[VolumeMount] = []

        for i, (volume_name, mount_path) in enumerate(
            self.config.persistent_volumes.items()
        ):
            self.persistent_volumes[f"executor-{i}"] = (volume_name, mount_path)

        self.container_templates: List[ContainerTemplate] = []
        self.templates: List[DagTemplate] = []
        self.template_defaults: Optional[TemplateDefaults] = None
        self.clean_names: dict = {}

    def prepare_for_graph_execution(self):
        """
        This method would be called prior to calling execute_graph.
        Perform any steps required before doing the graph execution.

        The most common implementation is to prepare a run log for the run if the run uses local interactive compute.

        But in cases of actual rendering the job specs (eg: AWS step functions, K8's) we need not do anything.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_traversal(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_traversal(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_traversal(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_traversal(self, self.experiment_tracker)

    def prepare_for_node_execution(self):
        """
        Perform any modifications to the services prior to execution of the node.

        Args:
            node (Node): [description]
            map_variable (dict, optional): [description]. Defaults to None.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_execution(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_execution(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_execution(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_execution(self, self.experiment_tracker)

        self._set_up_run_log(exists_ok=True)

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(
            node.name, node._get_step_log_name(map_variable)
        )

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        if self.dag:
            # functions and notebooks do not have dags
            _, current_branch = search_node_by_internal_name(
                dag=self.dag, internal_name=node.internal_name
            )
            _, next_node_name = self._get_status_and_next_node_name(
                node, current_branch, map_variable=map_variable
            )
            if next_node_name:
                # Terminal nodes do not have next node name
                next_node = current_branch.get_node_by_name(next_node_name)

                if next_node.node_type == defaults.FAIL:
                    self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(
            node._get_step_log_name(map_variable), self.run_id
        )
        if step_log.status == defaults.FAIL:
            raise Exception(f"Step {node.name} failed")

    def fan_out(self, node: BaseNode, map_variable: dict):
        super().fan_out(node, map_variable)

        # If its a map node, write the list values to "/tmp/magnus/output.txt"
        if node.node_type == "map":
            iterate_on = self.run_log_store.get_parameters(self.run_id)[node.iterate_on]

            with open("/tmp/output.txt", mode="w", encoding="utf-8") as myfile:
                json.dump(iterate_on, myfile, indent=4)

    def fan_in(self, node: BaseNode, map_variable: dict):
        super().fan_in(node, map_variable)

    def get_parameters(self):
        parameters = utils.get_user_set_parameters()
        if self.parameters_file:
            parameters.update(utils.load_yaml(self.parameters_file))
        return parameters

    def sanitize_name(self, name):
        return name.replace(" ", "-").replace(".", "-").replace("_", "-")

    def get_clean_name(self, node: BaseNode):
        # Cache names for the node
        if node.internal_name not in self.clean_names:
            sanitized = self.sanitize_name(node.name)
            tag = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            self.clean_names[node.internal_name] = f"{sanitized}-{node.node_type}-{tag}"

        return self.clean_names[node.internal_name]

    def compose_map_variable(
        self, list_of_iter_values: Optional[List] = None
    ) -> OrderedDict:
        map_variable = OrderedDict()

        # If we are inside a map node, compose a map_variable
        # The values of "iterate_as" are sent over as inputs to the container template
        if list_of_iter_values:
            for var in list_of_iter_values:
                map_variable[var] = "{{inputs.parameters." + str(var) + "}}"

        return map_variable

    def get_value_if_different_from_default(self, key, value):
        default_value = getattr(self.template_defaults, key)
        if default_value == value:
            return None
        return value

    def create_container_template(
        self,
        working_on: BaseNode,
        command: str,
        add_parameters: bool = False,
        inputs: List = None,
        outputs: List = None,
        overwrite_name: str = "",
    ):
        command = shlex.split(command)
        mode_config = self._resolve_executor_config(working_on)
        secrets = mode_config.get("secrets_from_k8s", {})

        docker_image = mode_config["docker_image"]
        cpu_limit = mode_config.get("cpu_limit", self.config.cpu_limit)
        memory_limit = mode_config.get("memory_limit", self.config.memory_limit)

        cpu_request = (
            mode_config.get("cpu_request", self.config.cpu_request) or cpu_limit
        )
        memory_request = (
            mode_config.get("memory_request", self.config.memory_request)
            or memory_limit
        )

        gpu_limit = mode_config.get("gpu_limit", self.config.gpu_limit)

        requests = Request(memory=memory_request, cpu=cpu_request)
        requests = self.get_value_if_different_from_default("requests", requests)

        limits = Limit(memory=memory_limit, cpu=cpu_limit, gpu=gpu_limit)
        limits = self.get_value_if_different_from_default("limits", limits)

        image_pull_policy = self.get_value_if_different_from_default(
            "imagePullPolicy",
            mode_config.get("image_pull_policy", self.config.image_pull_policy),
        )

        retry = self.get_value_if_different_from_default(
            "retry", working_on._get_max_attempts()
        )

        container = Container(
            command=command,
            image=docker_image,
            limits=limits,
            requests=requests,
            imagePullPolicy=image_pull_policy,
            retry=retry,
        )
        for secret_env, k8_secret in secrets.items():
            try:
                secret_name, key = k8_secret.split(":")
            except Exception as _e:
                msg = "K8's secret should be of format EnvVar=SecretName:Key"
                raise Exception(msg) from _e
            secret = SecretEnvVar(
                environment_variable=secret_env, secret_name=secret_name, secret_key=key
            )
            container.env.append(secret)

        if (
            add_parameters or working_on.name == self.dag.start_at
        ):  # short circuit when forcing to add parameters
            for key, value in self.get_parameters().items():
                # Get the value from work flow parameters for dynamic behavior
                if isinstance(value, dict) or isinstance(value, list):
                    continue
                env_var = EnvVar(
                    name=defaults.PARAMETER_PREFIX + key,
                    value="{{workflow.parameters." + key + "}}",
                )
                container.env.append(env_var)

        # Add the volume mounts to the container
        container.volumeMounts = self.volume_mounts

        clean_name = self.get_clean_name(working_on)
        if overwrite_name:
            clean_name = overwrite_name

        container_template = ContainerTemplate(name=clean_name, container=container)

        # inputs are the "iterate_as" value map variables in the same order as they are observed
        # We need to expose the map variables in the command of the container
        if inputs:
            container_template.inputs.extend(inputs)

        # The map step fan out would create an output that we should propagate via Argo
        if outputs:
            container_template.outputs.extend(outputs)

        return container_template

    def _create_fan_out_template(
        self, composite_node, list_of_iter_values: List = None
    ):
        clean_name = self.get_clean_name(composite_node)
        inputs = []
        # If we are fanning out already map state, we need to send the map variable inside
        # The container template also should be accepting an input parameter
        map_variable = None
        if list_of_iter_values:
            map_variable = self.compose_map_variable(
                list_of_iter_values=list_of_iter_values
            )

            for val in list_of_iter_values:
                inputs.append(Parameter(name=val))

        command = utils.get_fan_command(
            executor=self,
            mode="out",
            node=composite_node,
            run_id=self.run_id_placeholder,
            map_variable=map_variable,
        )

        outputs = []
        # If the node is a map node, we have to set the output parameters
        # Output is always the step's internal name + iterate-on
        if composite_node.node_type == "map":
            output_parameter = OutputParameter(name="iterate-on")
            outputs.append(output_parameter)

        # Create the node now
        step_config = {"command": command, "type": "task", "next": "dummy"}
        node = create_node(
            name=f"{composite_node.internal_name}-fan-out", step_config=step_config
        )

        container_template = self.create_container_template(
            working_on=node,
            command=command,
            outputs=outputs,
            inputs=inputs,
            overwrite_name=f"{clean_name}-fan-out",
        )

        self.container_templates.append(container_template)
        return TaskTemplate(
            name=f"{clean_name}-fan-out", template=f"{clean_name}-fan-out"
        )

    def _create_fan_in_template(self, composite_node, list_of_iter_values: List = None):
        clean_name = self.get_clean_name(composite_node)
        inputs = []
        # If we are fanning in already map state, we need to send the map variable inside
        # The container template also should be accepting an input parameter
        map_variable = None
        if list_of_iter_values:
            map_variable = self.compose_map_variable(
                list_of_iter_values=list_of_iter_values
            )

            for val in list_of_iter_values:
                inputs.append(Parameter(name=val))

        command = utils.get_fan_command(
            executor=self,
            mode="in",
            node=composite_node,
            run_id=self.run_id_placeholder,
            map_variable=map_variable,
        )

        step_config = {"command": command, "type": "task", "next": "dummy"}
        node = create_node(
            name=f"{composite_node.internal_name}-fan-in", step_config=step_config
        )
        container_template = self.create_container_template(
            working_on=node,
            command=command,
            inputs=inputs,
            overwrite_name=f"{clean_name}-fan-in",
        )
        self.container_templates.append(container_template)
        clean_name = self.get_clean_name(composite_node)
        return TaskTemplate(
            name=f"{clean_name}-fan-in", template=f"{clean_name}-fan-in"
        )

    def _gather_task_templates_of_dag(
        self, dag: Graph, dag_name="magnus-dag", list_of_iter_values: List = None
    ):
        current_node = dag.start_at
        previous_node = None
        previous_node_template_name = None

        templates: dict[str, TaskTemplate] = {}
        while True:
            working_on = dag.get_node_by_name(current_node)
            if previous_node == current_node:
                raise Exception("Potentially running in a infinite loop")

            render_obj = get_renderer(working_on)(executor=self, node=working_on)
            render_obj.render(list_of_iter_values=list_of_iter_values.copy())

            clean_name = self.get_clean_name(working_on)

            # If a task template for clean name exists, retrieve it (could have been created by on_failure)
            template = templates.get(
                clean_name, TaskTemplate(name=clean_name, template=clean_name)
            )

            # Link the current node to previous node, if the previous node was successful.
            if previous_node:
                template.depends.append(f"{previous_node_template_name}.Succeeded")

            templates[clean_name] = template

            # On failure nodes
            if working_on._get_on_failure_node():
                failure_node = dag.get_node_by_name(working_on._get_on_failure_node())

                failure_template_name = self.get_clean_name(failure_node)
                # If a task template for clean name exists, retrieve it
                failure_template = templates.get(
                    failure_template_name,
                    TaskTemplate(
                        name=failure_template_name, template=failure_template_name
                    ),
                )
                failure_template.depends.append(f"{clean_name}.Failed")

                templates[failure_template_name] = failure_template

            # If we are in a map node, we need to add the values as arguments
            template = templates[clean_name]
            if list_of_iter_values:
                for value in list_of_iter_values:
                    template.arguments.append(
                        Parameter(
                            name=value, value="{{inputs.parameters." + value + "}}"
                        )
                    )

            # Move ahead to the next node
            previous_node = current_node
            previous_node_template_name = self.get_clean_name(working_on)

            if working_on.node_type in ["success", "fail"]:
                break

            current_node = working_on._get_next_node()

        # Add the iteration values as input to dag template
        dag_template = DagTemplate(tasks=list(templates.values()), name=dag_name)
        if list_of_iter_values:
            dag_template.inputs.extend(
                [Parameter(name=val) for val in list_of_iter_values]
            )

        # Add the dag template to the list of templates
        self.templates.append(dag_template)

    def _add_volumes_to_specification(self, specification: Spec):
        visited_claims = {}

        for volume_name, claim in self.persistent_volumes.items():
            claim_name, mount_path = claim

            # If the volume is already mounted, we cannot mount it again.
            if claim_name in visited_claims:
                msg = "The same persistent volume claim has already been used in the pipeline by another service"
                raise Exception(msg)
            visited_claims[claim_name] = claim_name
            specification.volumes.append(Volume(name=volume_name, claim=claim_name))
            self.volume_mounts.append(
                VolumeMount(name=volume_name, mountPath=mount_path)
            )

    def _fill_template_defaults(self):
        cpu_limit = self.config.cpu_limit
        memory_limit = self.config.memory_limit

        cpu_request = cpu_limit
        memory_request = memory_limit

        gpu_limit = self.config.gpu_limit

        request = Request(memory=memory_request, cpu=cpu_request)
        limits = Limit(memory=memory_limit, cpu=cpu_limit, gpu=gpu_limit)

        self.template_defaults = TemplateDefaults(
            limits=limits,
            requests=request,
            imagePullPolicy=self.config.image_pull_policy,
            volumeMounts=self.volume_mounts,
        )

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        workspec = WorkSpec()
        specification = Spec()
        workspec.spec = specification

        for key, value in self.get_parameters().items():
            # Get the value from work flow parameters for dynamic behavior
            env_var = EnvVar(name=key, value=value)
            if isinstance(value, dict) or isinstance(value, list):
                continue
            specification.arguments.append(env_var)

        run_id_var = EnvVar(name="run_id", value="{{workflow.uid}}")
        specification.arguments.append(run_id_var)

        if self.config.parallelism:
            specification.parallelism = self.config.parallelism

        # Add volumes to specification
        self._add_volumes_to_specification(specification)

        specification.nodeSelector = self.config.node_selectors
        specification.tolerations = self.config.tolerations

        # Fill in the template defaults
        self._fill_template_defaults()
        specification.templateDefaults = self.template_defaults

        # Container specifications are globally collected and added at the end.
        # Dag specifications are added as part of the dag traversal.
        self._gather_task_templates_of_dag(dag=dag, list_of_iter_values=[])
        specification.templates.extend(self.templates)
        specification.templates.extend(self.container_templates)

        yaml = YAML()
        with open(self.config.output_file, "w") as f:
            yaml.dump(workspec.dict(), f)

    def execute_job(self, node: BaseNode):
        """
        This is not the best use of this executor but it works!!
        """
        workspec = WorkSpec()
        specification = Spec()
        workspec.spec = specification

        for key, value in self.get_parameters().items():
            # Get the value from work flow parameters for dynamic behavior
            if isinstance(value, List) or isinstance(value, dict):
                # Do not do this for complex data types as they cannot be sent via UI
                continue
            env_var = EnvVar(name=key, value=value)
            specification.arguments.append(env_var)

        run_id_var = EnvVar(name="run_id", value="{{workflow.uid}}")
        specification.arguments.append(run_id_var)

        # Add volumes to specification
        self._add_volumes_to_specification(specification)

        command = utils.get_job_execution_command(
            self, node, over_write_run_id=self.run_id_placeholder
        )
        container_template = self.create_container_template(
            working_on=node, command=command, add_parameters=True
        )

        clean_name = self.get_clean_name(node)
        template = DagTemplate(name=clean_name, template=clean_name)

        specification.templates.extend([container_template])
        dag_template = DagTemplate(tasks=[template])
        specification.templates.extend([dag_template])
        yaml = YAML()
        with open(self.config.output_file, "w") as f:
            yaml.dump(workspec.dict(), f)

    def send_return_code(self, stage="traversal"):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if (
            stage != "traversal"
        ):  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception("Pipeline execution failed")
