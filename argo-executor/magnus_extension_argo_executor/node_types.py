import re
from typing import List
from collections import defaultdict

from pydantic import BaseModel

from magnus.nodes import BaseNode
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus import utils


class ArgoTemplate(BaseModel):
    """
    An argo template model
    """
    node: BaseNode = None
    name: str
    command: List = []
    image: str = None
    resources: dict = {}
    template: str = None
    dag: 'ArgoDag' = None
    dependencies: List = []
    depends: str = None
    inputs: dict = {}
    outputs: dict = {}

    class Config:
        arbitrary_types_allowed = True
        fields = {'node': {'exclude': True}}


class ArgoDag(BaseModel):
    """
    An argo dag model
    """
    tasks: List[ArgoTemplate] = []


class BaseNodeType:
    """
    The base node type
    """

    def __init__(self, node: BaseNode, dag: Graph, executor: BaseExecutor):
        self.node = node
        self.dag = dag
        self.executor = executor

    def get_argo_step_name(self, name: str) -> str:
        """
        Given a step name, return its step name in argo format

        Args:
            name (str): The name that we want the argo step name for.

        Returns:
            str: The argo step name
        """

        return_value = re.sub(r'\s+', '-', name)
        return_value = re.sub(r'\.', '-', return_value)

        return return_value

    @property
    def argo_step_name(self) -> str:
        """
        Given an internal step name, convert it into argo friendly step name.

        Replace:
            . with -
            ' ' with -

        Args:
            internal_step_name (_type_): argo friendly step name
        """
        return self.get_argo_step_name(self.node.internal_name)

    @property
    def argo_template_name(self) -> str:
        """
        Return the argo template name of the step

        Returns:
            str: The template name in the argo workspec for a step
        """
        return self.argo_step_name + '-template'

    @property
    def dependant(self) -> str:
        """
        Return the dependant of this step.

        Returns:
            List[str]: The argo step name that self.node depends upon.
        """
        next_node = self.node.get_next_node()

        if not next_node:
            return None  #  For terminal nodes

        next_node_obj = self.dag.get_node_by_name(next_node)

        if next_node_obj.is_terminal_node():
            return None

        return self.get_argo_step_name(next_node_obj.internal_name)

    def get_templates(self) -> List[ArgoTemplate]:
        """
        Returns the argo template for the node

        Returns:
            List[ArgoTemplate]: List of argo templates corresponding to the node
        """
        raise NotImplementedError


class Task(BaseNodeType):
    node_type = 'task'

    def _validate(self):
        effective_node_config = self.executor.resolve_node_config(node=self.node)

        if 'image' not in effective_node_config:
            raise Exception('image should be specified either at the global config level or step level')

    @property
    def image(self):
        effective_node_config = self.executor.resolve_node_config(node=self.node)

        return effective_node_config['image']

    @property
    def resources(self):
        effective_node_config = self.executor.resolve_node_config(node=self.node)

        return effective_node_config.get('resources', {})

    def get_templates(self) -> List[ArgoTemplate]:
        self._validate()

        node_execution_command = utils.get_node_execution_command(
            node=self.node, over_write_run_id='{{workflow.uid}}', executor=self.executor)

        node_template = ArgoTemplate(node=self.node,
                                     name=self.argo_template_name,
                                     command=node_execution_command.split(),
                                     template='default_template',
                                     image=self.image,
                                     resources=self.resources
                                     )

        return [node_template]


class Success(BaseNodeType):
    node_type = 'success'

    def get_templates(self) -> List[ArgoTemplate]:
        # We do not render this step, so not returning anything
        return []


class Fail(BaseNodeType):
    node_type = 'fail'

    def get_templates(self) -> List[ArgoTemplate]:
        # We do not render this step, so not returning anything
        return []


class Parallel(BaseNodeType):
    node_type = 'parallel'

    def get_templates(self) -> List[ArgoTemplate]:
        """
        One step in the base dag, called by the task name and refers to a template.

        The template itself is a dag which has all the branches as tasks.
        All the tasks are themselves templates.

        Returns:
            List[ArgoTemplate]: _description_
        """
        templates = []

        parallel_template = ArgoTemplate(name=self.argo_template_name)
        parallel_template.dag = ArgoDag()

        for branch_name, branch in self.node.branches.items():
            branch_task = ArgoTemplate(
                name=self.get_argo_step_name(branch_name),
                template=self.get_argo_step_name(branch_name) + '-template')
            parallel_template.dag.tasks.append(branch_task)

            branch_templates, branch_template = get_dag_templates(
                dag=branch, template_name=self.get_argo_step_name(branch_name) + '-template', executor=self.executor)

            templates.extend(branch_templates)
            templates.append(branch_template)

        templates.append(parallel_template)
        return templates

# class Map(BaseNodeType):
#     node_type = 'map'

# class Dag(BaseNodeType):
#     node_type = 'dag'

# class AsIs(BaseNodeType):
#     node_type = 'as-is'


def get_node_class(node_type):
    for subclass in BaseNodeType.__subclasses__():
        if subclass.node_type == node_type:
            return subclass

    raise Exception('Unidentified Node type')


# TODO: Need to accommodate on_fail
def get_dag_templates(dag, template_name, executor):
    dependencies = defaultdict(list)  # if A.next = B, dependencies[B] = [A]

    dag_template = ArgoTemplate(name=template_name)
    dag_template.dag = ArgoDag()

    current_node = dag.start_at
    previous_node = None

    templates = []
    while True:
        working_on = dag.get_node_by_name(current_node)

        if working_on.node_type in ['success', 'fail']:
            break

        if previous_node == current_node:
            raise Exception('Potentially running in a infinite loop')

        previous_node = current_node

        node_class = get_node_class(working_on.node_type)
        node_obj = node_class(node=working_on, dag=dag, executor=executor)

        # Add the node templates to workspec
        node_templates = node_obj.get_templates()
        templates.extend(node_templates)

        # maintain dependencies
        if node_obj.dependant:
            dependencies[node_obj.dependant].append(node_obj.argo_step_name)

        # Add the task templates
        task_template = ArgoTemplate(name=node_obj.argo_step_name, template=node_obj.argo_template_name)
        task_template.dependencies = dependencies[node_obj.argo_step_name]
        dag_template.dag.tasks.append(task_template)

        # Move forward
        current_node = working_on.get_next_node()

    return templates, dag_template
