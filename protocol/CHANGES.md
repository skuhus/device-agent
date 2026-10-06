# Protocol changes

The protocol's version is `info.version` in asyncapi.yaml, and its major
version is the `schema` field of every message; asyncapi.yaml, "Versions", has
the rules. Each version is released as `protocol-v<version>`, with
asyncapi.yaml, messages.schema.json, this file and a rendered page.

From 2.2.0 the keepalive says which version the agent speaks, in
`protocol_version`. An agent before it speaks the version its `agent_version`
maps to:

| Protocol | Agent |
|---|---|
| 2.2.0 | the first agent release after 2.1.0, and later |
| 2.1.0 | 2.1.0 |
| 2.0.0 | 2.0.0 |

## 2.2.0

- The keepalive carries `protocol_version`, the protocol version the agent
  speaks.
- The first version published on its own, as asyncapi.yaml and
  messages.schema.json.

## 2.1.0

- Broadcast groups: a tx on a group's topic is written to every device in the
  group (#35). The topics are `skuhus/<project>/group/<group>/tx`,
  `skuhus/<project>/<site>/group/<group>/tx` and
  `skuhus/<project>/<site>/<station>/group/<group>/tx`.
- Each device in the keepalive carries `tx_topics`: every topic that reaches the
  device's tx, with the broker's answer to the subscription.
- `group` is reserved, and a station cannot take it as its id.

## 2.0.0

The first version of schema 2: the topics `rx`, `tx` and `status` of each
device, and the agent's own `status` topic, with the messages `rx`, `event`,
`tx`, `tx_result`, `keepalive` and `offline`. v1's messages, schema 1, are not
part of it.
