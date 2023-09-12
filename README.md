## dbt-rpc

> :warning: **The `dbt-rpc` plugin is deprecated.**
>
> dbt Labs actively maintained `dbt-rpc` for compatibility with dbt-core versions up to v1.5. Starting with dbt-core v1.6 (released in July 2023), `dbt-rpc` is no longer supported for ongoing compatibility. In the meantime, dbt Labs will be performing critical maintenance only for `dbt-rpc`, until the last compatible version of dbt-core has reached the end of official support (see [version policies](https://docs.getdbt.com/docs/dbt-versions/core)). At that point, dbt Labs will archive this repository to be read-only.

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
