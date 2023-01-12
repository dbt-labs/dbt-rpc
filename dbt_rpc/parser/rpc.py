import os
from dataclasses import dataclass
from typing import Iterable, Optional

from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.nodes import RPCNode, Macro
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.exceptions import DbtInternalError
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.macros import MacroParser
from dbt.parser.search import FileBlock


@dataclass
class RPCBlock(FileBlock):
    rpc_name: str

    @property
    def name(self):
        return self.rpc_name


class RPCCallParser(SimpleSQLParser[RPCNode]):
    def parse_from_dict(self, dct, validate=True) -> RPCNode:
        if validate:
            RPCNode.validate(dct)
        return RPCNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.RPCCall

    def get_compiled_path(cls, block: FileBlock):
        # we do it this way to make mypy happy
        if not isinstance(block, RPCBlock):
            raise DbtInternalError(
                'While parsing RPC calls, got an actual file block instead of '
                'an RPC block: {}'.format(block)
            )

        return os.path.join('rpc', block.name)

    def parse_remote(self, sql: str, name: str, language: Optional[str] = 'sql') -> RPCNode:
        source_file = SourceFile.remote(sql, self.project.project_name, language)
        contents = RPCBlock(rpc_name=name, file=source_file)
        return self.parse_node(contents)


class RPCMacroParser(MacroParser):
    def parse_remote(self, contents) -> Iterable[Macro]:
        base = UnparsedMacro(
            path='from remote system',
            original_file_path='from remote system',
            package_name=self.project.project_name,
            raw_code=contents,
            language='sql',
            resource_type=NodeType.Macro,
        )
        for node in self.parse_unparsed_macros(base):
            yield node
