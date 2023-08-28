import json
import logging
import random
import shlex
import string
from collections import OrderedDict
from typing import Any, List, Optional, Union

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph, create_node, search_node_by_internal_name
from magnus.nodes import BaseNode
from pydantic import BaseModel, Extra, Field
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
            "valueFrom": {"secretKeyRef": {"name": self.secret_name, "key": self.secret_key}},
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
    The default requests
    """

    memory: str = "1Gi"
    cpu: str = "250m"


class Retry(BaseModel):
    limit: int = 0
    retryPolicy: str = "Always"

    def dict(self, *args, **kwargs) -> dict:
        return {"limit": str(self.limit), "retryPolicy": self.retryPolicy}


class Limit(Request):
    """
    The default limits
    """

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


class Toleration(BaseModel):
    effect: str
    key: str
    operator: str
    value: str


class TemplateDefaults(BaseModel):
    limits: Limit = Limit()
    requests: Request = Request()
    imagePullPolicy: str = Field("", alias="imagePullPolicy")  # "Always", "IfNotPresent", "Never"
    # https://argoproj.github.io/argo-workflows/fields/#container
    nodeSelector: dict = Field({}, alias="nodeSelector")
    tolerations: List[Toleration] = []
    # retry seems to mess with the graph viz but works well with gant chart.
    retryStrategy: Retry = Field(Retry(), alias="retryStrategy")
    activeDeadlineSeconds: int = Field(60 * 60 * 2, alias="activeDeadlineSeconds", gt=0)
    # Volume mounts cannot be defined here


class Container(BaseModel):
    """
    Container over-rides from the template defaults
    """

    class Config:
        # Needed for executor_config overrides
        extra = Extra.allow

    command: List[str]
    image: str
    env: List[Union[SecretEnvVar, EnvVar]] = []
    volumeMounts: List[VolumeMount] = []


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
    with_param: Optional[str] = None  # If its part of a map node

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
    parallelism: int = 0
    failFast: bool = True

    def dict(self, *args, **kwargs):
        rv = {
            "name": self.name,
            "dag": {"tasks": [task.dict() for task in self.tasks]},
        }
        if self.inputs:
            rv["inputs"] = {"parameters": [param.dict() for param in self.inputs]}

        if self.parallelism:
            rv["parallelism"] = self.parallelism

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
    templateDefaults: Optional[TemplateDefaults] = None
    parallelism: int = 0

    def dict(self, *args, **kwargs):
        return_value = super().dict(*args, **kwargs)

        arguments = return_value.pop("arguments", [])
        return_value["arguments"] = {"parameters": arguments}

        if not self.parallelism:
            del return_value["parallelism"]

        return return_value


class WorkSpec(BaseModel):
    apiVersion: str = "argoproj.io/v1alpha1"
    kind: str = "Workflow"
    metadata: dict = {"generateName": "magnus-dag-"}
    spec: Spec = Spec()


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
                task_template_arguments.append(Parameter(name=value, value="{{inputs.parameters." + value + "}}"))
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
                task_template_arguments.append(Parameter(name=value, value="{{inputs.parameters." + value + "}}"))
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
                task_template_arguments.append(Parameter(name=value, value="{{inputs.parameters." + value + "}}"))
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
        task_template.with_param = "{{tasks." + f"{clean_name}-fan-out" + ".outputs.parameters." + "iterate-on" + "}}"

        argument = Argument(name=self.node.iterate_as, value="{{item}}")
        task_template.arguments.append(argument)

        task_template.depends.append(f"{clean_name}-fan-out.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-map.Succeeded")
        fan_in_template.depends.append(f"{clean_name}-map.Failed")

        executor_config = self.executor._resolve_executor_config(self.node)

        self.executor.templates.append(
            DagTemplate(
                tasks=[fan_out_template, task_template, fan_in_template],
                name=clean_name,
                inputs=dag_inputs,
                parallelism=executor_config.get("parallelism", 0),
                failFast=executor_config.get("failFast", True),
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

    class Config(BaseExecutor.Config, TemplateDefaults):
        image: str = Field(alias="image")
        parallelism: int = 0
        output_file: str = "argo-pipeline.yaml"
        secrets_from_k8s: dict = {}  # EnvVar=SecretName:Key
        persistent_volumes: dict = {}
        failFast: bool = Field(True, alias="failFast")  # Only applies to withItems or map state

    def __init__(self, config: dict = None):
        super().__init__(config)

        self.persistent_volumes = {}
        self.volume_mounts: List[VolumeMount] = []

        for i, (volume_name, mount_path) in enumerate(self.config.persistent_volumes.items()):
            self.persistent_volumes[f"executor-{i}"] = (volume_name, mount_path)

        self.container_templates: List[ContainerTemplate] = []
        self.templates: List[DagTemplate] = []
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
        step_log = self.run_log_store.create_step_log(node.name, node._get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        if self.dag:
            # functions and notebooks do not have dags
            _, current_branch = search_node_by_internal_name(dag=self.dag, internal_name=node.internal_name)
            _, next_node_name = self._get_status_and_next_node_name(node, current_branch, map_variable=map_variable)
            if next_node_name:
                # Terminal nodes do not have next node name
                next_node = current_branch.get_node_by_name(next_node_name)

                if next_node.node_type == defaults.FAIL:
                    self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(node._get_step_log_name(map_variable), self.run_id)
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

    def compose_map_variable(self, list_of_iter_values: Optional[List] = None) -> OrderedDict:
        map_variable = OrderedDict()

        # If we are inside a map node, compose a map_variable
        # The values of "iterate_as" are sent over as inputs to the container template
        if list_of_iter_values:
            for var in list_of_iter_values:
                map_variable[var] = "{{inputs.parameters." + str(var) + "}}"

        return map_variable

    def _fill_container_overrides(self, mode_config: dict, container: Container) -> dict:
        executor_config = self._get_template_defaults()
        # mode_config has executor config + container over-rides
        container_config = self._get_template_defaults(check_against=mode_config)

        for key in list(container_config.__fields__.keys()):
            container_value = container_config.__getattribute__(key)
            executor_value = executor_config.__getattribute__(key)

            if container_value != executor_value:
                container.__setattr__(key, container_value)

    def create_container_template(
        self,
        working_on: BaseNode,
        command: str,
        inputs: List = None,
        outputs: List = None,
        overwrite_name: str = "",
    ):
        mode_config = self._resolve_executor_config(working_on)

        image = mode_config["image"]
        command = shlex.split(command)

        container_init_kwargs = {"image": image, "command": command}
        container = Container(**container_init_kwargs)

        self._fill_container_overrides(mode_config=mode_config, container=container)

        secrets = mode_config.get("secrets_from_k8s", {})
        for secret_env, k8_secret in secrets.items():
            try:
                secret_name, key = k8_secret.split(":")
            except Exception as _e:
                msg = "K8's secret should be of format EnvVar=SecretName:Key"
                raise Exception(msg) from _e
            secret = SecretEnvVar(environment_variable=secret_env, secret_name=secret_name, secret_key=key)
            container.env.append(secret)

        if working_on.name == self.dag.start_at:
            for key, value in self.get_parameters().items():
                # Get the value from work flow parameters for dynamic behavior
                if isinstance(value, dict) or isinstance(value, list):
                    continue
                env_var = EnvVar(
                    name=defaults.PARAMETER_PREFIX + key,
                    value="{{workflow.parameters." + key + "}}",
                )
                container.env.append(env_var)

        clean_name = self.get_clean_name(working_on)
        if overwrite_name:
            clean_name = overwrite_name

        container.volumeMounts = self.volume_mounts

        container_template = ContainerTemplate(name=clean_name, container=container)

        # inputs are the "iterate_as" value map variables in the same order as they are observed
        # We need to expose the map variables in the command of the container
        if inputs:
            container_template.inputs.extend(inputs)

        # The map step fan out would create an output that we should propagate via Argo
        if outputs:
            container_template.outputs.extend(outputs)

        return container_template

    def _create_fan_out_template(self, composite_node, list_of_iter_values: List = None):
        clean_name = self.get_clean_name(composite_node)
        inputs = []
        # If we are fanning out already map state, we need to send the map variable inside
        # The container template also should be accepting an input parameter
        map_variable = None
        if list_of_iter_values:
            map_variable = self.compose_map_variable(list_of_iter_values=list_of_iter_values)

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
        node = create_node(name=f"{composite_node.internal_name}-fan-out", step_config=step_config)

        container_template = self.create_container_template(
            working_on=node,
            command=command,
            outputs=outputs,
            inputs=inputs,
            overwrite_name=f"{clean_name}-fan-out",
        )

        self.container_templates.append(container_template)
        return TaskTemplate(name=f"{clean_name}-fan-out", template=f"{clean_name}-fan-out")

    def _create_fan_in_template(self, composite_node, list_of_iter_values: List = None):
        clean_name = self.get_clean_name(composite_node)
        inputs = []
        # If we are fanning in already map state, we need to send the map variable inside
        # The container template also should be accepting an input parameter
        map_variable = None
        if list_of_iter_values:
            map_variable = self.compose_map_variable(list_of_iter_values=list_of_iter_values)

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
        node = create_node(name=f"{composite_node.internal_name}-fan-in", step_config=step_config)
        container_template = self.create_container_template(
            working_on=node,
            command=command,
            inputs=inputs,
            overwrite_name=f"{clean_name}-fan-in",
        )
        self.container_templates.append(container_template)
        clean_name = self.get_clean_name(composite_node)
        return TaskTemplate(name=f"{clean_name}-fan-in", template=f"{clean_name}-fan-in")

    def _gather_task_templates_of_dag(self, dag: Graph, dag_name="magnus-dag", list_of_iter_values: List = None):
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
            template = templates.get(clean_name, TaskTemplate(name=clean_name, template=clean_name))

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
                    TaskTemplate(name=failure_template_name, template=failure_template_name),
                )
                failure_template.depends.append(f"{clean_name}.Failed")

                templates[failure_template_name] = failure_template

            # If we are in a map node, we need to add the values as arguments
            template = templates[clean_name]
            if list_of_iter_values:
                for value in list_of_iter_values:
                    template.arguments.append(Parameter(name=value, value="{{inputs.parameters." + value + "}}"))

            # Move ahead to the next node
            previous_node = current_node
            previous_node_template_name = self.get_clean_name(working_on)

            if working_on.node_type in ["success", "fail"]:
                break

            current_node = working_on._get_next_node()

        # Add the iteration values as input to dag template
        dag_template = DagTemplate(tasks=list(templates.values()), name=dag_name)
        if list_of_iter_values:
            dag_template.inputs.extend([Parameter(name=val) for val in list_of_iter_values])

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
            self.volume_mounts.append(VolumeMount(name=volume_name, mountPath=mount_path))

    def _get_template_defaults(self, check_against: dict = {}) -> TemplateDefaults:
        template_defaults_keys = list(TemplateDefaults.__fields__.keys())

        user_provided_config = self.config.dict()
        if check_against:
            user_provided_config = check_against

        template_default_json = {key: user_provided_config[key] for key in template_defaults_keys}

        return TemplateDefaults(**template_default_json)

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        workspec = WorkSpec()
        specification = workspec.spec

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

        # Fill in the template defaults
        specification.templateDefaults = self._get_template_defaults()

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
        Use K8's job instead
        """
        raise NotImplementedError("Use K8's job instead")

    def send_return_code(self, stage="traversal"):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if stage != "traversal":  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception("Pipeline execution failed")
