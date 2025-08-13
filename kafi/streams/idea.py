from IPython.display import display, Markdown

class SQLProcessor:
    def process(self, sql_code, input_topics, output_topics):
        """
        Verarbeite den SQL-Code mit den Input- und Output-Topics.

        :param sql_code: String mit SQL-Code
        :param input_topics: Liste von Input-Topics
        :param output_topics: Liste von Output-Topics
        """
        raise NotImplementedError("Bitte implementiere diesen Prozess")


class KStream:
    _id_counter = 0

    def __init__(self, input_topics, output_topics=None):
        self.input_topics = list(input_topics)
        self.output_topics = list(output_topics) if output_topics else []
        self.operations = []
        self.children = []
        self._id = KStream._id_counter
        KStream._id_counter += 1

    @classmethod
    def from_topics(cls, *topics):
        return cls(topics)

    def set_input_topics(self, *topics):
        self.input_topics = list(topics)
        return self

    def set_output_topics(self, *topics):
        self.output_topics = list(topics)
        return self

    def _add_child(self, child):
        self.children.append(child)

    def filter(self, predicate):
        self.operations.append(('filter', predicate))
        return self

    def map(self, mapper, input_topics=None, output_topics=None):
        self.operations.append(('map', mapper))
        new_input = input_topics if input_topics is not None else self.input_topics
        new_output = output_topics if output_topics is not None else self.output_topics
        new_stream = KStream(new_input, new_output)
        new_stream.operations = self.operations.copy()
        self._add_child(new_stream)
        return new_stream

    def flat_map(self, mapper, input_topics=None, output_topics=None):
        self.operations.append(('flat_map', mapper))
        new_input = input_topics if input_topics is not None else self.input_topics
        new_output = output_topics if output_topics is not None else self.output_topics
        new_stream = KStream(new_input, new_output)
        new_stream.operations = self.operations.copy()
        self._add_child(new_stream)
        return new_stream

    def peek(self, action):
        self.operations.append(('peek', action))
        return self

    def group_by(self, key_selector, input_topics=None, output_topics=None):
        self.operations.append(('group_by', key_selector))
        new_input = input_topics if input_topics is not None else self.input_topics
        new_output = output_topics if output_topics is not None else self.output_topics
        new_stream = KStream(new_input, new_output)
        new_stream.operations = self.operations.copy()
        self._add_child(new_stream)
        return new_stream

    def reduce(self, reducer):
        self.operations.append(('reduce', reducer))
        return self

    def join(self, other, joiner, window_ms=5000, input_topics=None, output_topics=None):
        self.operations.append(('join', (other, joiner, window_ms)))
        combined_input = input_topics if input_topics is not None else list(set(self.input_topics + other.input_topics))
        combined_output = output_topics if output_topics is not None else self.output_topics
        new_stream = KStream(combined_input, combined_output)
        new_stream.operations = self.operations.copy()
        self._add_child(new_stream)
        return new_stream

    def branch(self, *predicates):
        branches = []
        for pred in predicates:
            branch_stream = KStream(self.input_topics, self.output_topics)
            branch_stream.operations = self.operations.copy()
            branch_stream.operations.append(('branch_filter', pred))
            self._add_child(branch_stream)
            branches.append(branch_stream)
        return branches

    def to(self, *topics):
        self.output_topics.extend(topics)
        return self

    def sql(self, sql_code, input_topics=None, output_topics=None, processor=None):
        in_topics = input_topics if input_topics is not None else self.input_topics
        out_topics = output_topics if output_topics is not None else []
        self.operations.append(('sql', sql_code))
        new_stream = KStream(in_topics, out_topics)
        new_stream.operations = self.operations.copy()
        self._add_child(new_stream)

        if processor:
            processor.process(sql_code, in_topics, out_topics)

        return new_stream

    def _op_to_str(self, op):
        name, arg = op
        if name == 'sql':
            snippet = (arg[:30] + '...') if len(arg) > 30 else arg
            return f"sql(\"{snippet}\")"
        if name in ['filter', 'peek', 'group_by', 'branch_filter']:
            return f"{name}({getattr(arg, '__name__', 'lambda')})"
        if name in ['map', 'flat_map']:
            return f"{name}({getattr(arg, '__name__', 'lambda')})"
        if name == 'reduce':
            return f"reduce({getattr(arg, '__name__', 'lambda')})"
        if name == 'join':
            other, _, window = arg
            return f"join(topics={other.input_topics}, window_ms={window})"
        return name

    def _mermaid_label(self):
        ops = [self._op_to_str(op) for op in self.operations]
        ops_str = "\\n".join(ops) if ops else "(no ops)"
        inputs = ", ".join(self.input_topics) if self.input_topics else "(no input)"
        outputs = ", ".join(self.output_topics) if self.output_topics else "(no output)"
        label = f"ID {self._id}\\nInput: {inputs}\\nOps:\\n{ops_str}\\nOutput: {outputs}"
        return label

    def _build_mermaid_nodes_edges(self):
        nodes = []
        edges = []

        def recurse(stream):
            node_id = f"n{stream._id}"
            nodes.append(f'{node_id}["{stream._mermaid_label()}"]')
            for child in stream.children:
                child_id = f"n{child._id}"
                edges.append(f"{node_id} --> {child_id}")
                recurse(child)

        recurse(self)
        return nodes, edges

    def display_mermaid_topology(self):
        nodes, edges = self._build_mermaid_nodes_edges()
        mermaid_code = "```mermaid\nflowchart TD\n"
        mermaid_code += "\n".join(nodes) + "\n"
        mermaid_code += "\n".join(edges) + "\n"
        mermaid_code += "```"
        display(Markdown(mermaid_code))


# Beispiel-Implementierung eines SQL-Processors
class MySQLProcessor(SQLProcessor):
    def process(self, sql_code, input_topics, output_topics):
        print(f"Registriere SQL-Job:\nSQL:\n{sql_code.strip()}\nInput Topics: {input_topics}\nOutput Topics: {output_topics}")
        # Hier könnte die Übergabe an eine externe Stream-Engine erfolgen


# Beispiel-Nutzung

if __name__ == "__main__":
    processor = MySQLProcessor()

    stream = KStream.from_topics("topic1", "topic2")

    sql_stream = stream.sql(
        """
        SELECT key, SUM(value) FROM topic1 JOIN topic2 ON key GROUP BY key
        """,
        input_topics=["topic1", "topic2"],
        output_topics=["result_topic"],
        processor=processor
    )

    sql_stream.display_mermaid_topology()
