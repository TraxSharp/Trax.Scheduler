---
authors: [Theauxm]
areas: [scheduling, platform]
status: accepted
---

# Remote execution crosses the process boundary as JSON, keyed by the canonical train name

A run dispatched to a remote worker travels as `RemoteRunRequest(TrainName, InputJson,
InputType)`: the train's fully qualified service type name, the input serialized to JSON,
and the input's type name so the far side can deserialize it. Nothing about the train's
compiled type crosses the boundary.

## Status

**Accepted.**

## Considered options

**Binary serialization of the input object.** Faster and type-safe at both ends, and
rejected because both ends would then have to ship the same assemblies at the same version.
That is exactly the coupling remote execution exists to avoid: a Lambda runner should be
deployable without redeploying the scheduler.

**Sending the concrete train type.** Rejected for the reason
`Trax.Docs/adr/0007-the-canonical-train-name-is-the-interface-fullname.md` gives: the
concrete type is an implementation detail that can be renamed or swapped, while the
interface FullName is the identifier every other layer already stores.

## Consequences

**The contract is a public record, and changing it is a breaking change for anyone running
a worker built against the old shape.** Adding a property is safe; renaming or removing one
is not, and neither end validates a version.

**`InputType` is a string the far side resolves.** A worker that does not have the input
type loaded fails at deserialization, with an error naming a type rather than naming the
train. That is the cost of decoupling the assemblies.

**The train name in the request is the same string stored in `work_queue.train_name` and
`manifest.Name`,** so a remote run and a local one are the same job as far as every query
and every dashboard view is concerned.

## Exemplars

- `RemoteRunContractTests` round-trips both the request and the response through JSON and
  pins record equality, so a property rename or a reordering that breaks the wire shape
  fails here rather than in a deployed worker.

Not covered: nothing checks that the two ends agree on a version, because there is no
version field. A worker built against an older contract and a scheduler sending a newer one
will deserialize whatever matches and silently drop the rest.

## Changelog

- **2026-09-11**: Recorded.
