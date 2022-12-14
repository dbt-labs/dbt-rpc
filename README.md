## dbt-rpc

> :warning: **The `dbt-rpc` plugin will be fully deprecated by the second half of of 2023.**
>
> dbt Labs is actively maintaining `dbt-rpc` up to dbt v1.4. Starting in v1.5, we intend to break `dbt-rpc` compatibility in favor of [the new dbt Server](https://github.com/dbt-labs/dbt-server). dbt Labs will perform critical maintenance only, until the last compatible version of dbt has reached end of official support (12 months after release; [see Core version policies](https://docs.getdbt.com/docs/dbt-versions/core)) -- then we plan to read-only archive this repository.

This plugin introduces a `dbt-rpc serve` command, which runs a Remote Procedure Call Server that enables you to submit dbt commands in a programmatic way. (This command is equivalent to the `dbt rpc` command that was available in older versions of `dbt-core`.)

This plugin requires `dbt-core` and `json-rpc`. You should install it alongside an [adapter plugin](https://docs.getdbt.com/docs/available-adapters) for use with your database/platform/engine.

## Links

- [Documentation](https://docs.getdbt.com/reference/commands/rpc)
- History: [dbt-core#1141](https://github.com/dbt-labs/dbt/issues/1141), [dbt-core#1274](https://github.com/dbt-labs/dbt/issues/1274), [dbt-core#1301](https://github.com/dbt-labs/dbt/pull/1301)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/dbt-labs/dbt-rpc/issues/new)
- Want to help us build dbt Core? Check out the [Contributing Guide](https://github.com/dbt-labs/dbt/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
