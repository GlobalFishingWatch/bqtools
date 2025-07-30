from gfw.common.cli import CLI, Option
from gfw.common.logging import LoggerConfig
from src.copy_dataset import copy_dataset_command
from src.export_dataset import export_dataset_command
import logging
import sys


logger = logging.getLogger(__name__)


HELP_DRY_RUN = "If passed, all queries, if any, will be run in dry run mode."


def cli(args):

    migration_tools = CLI(
        name="migration_tools",
        description="Migration tools.",
        options=[
            Option("--dry-run", type=bool, default=False, help=HELP_DRY_RUN),
        ],
        subcommands=[
            copy_dataset_command,
            export_dataset_command,
        ],
        version='0.1.0',
        examples=[
            "migration_tools copy_dataset --source_project p1 --source_dataset d1 --dest_project p2 --dest_dataset d2",
        ],
        allow_unknown=False,
        use_underscore=True,
    )

    return migration_tools.execute(args)


def main():
    logger.info('Started')
    r = cli(sys.argv[1:])
    logger.info('Finished')
    sys.exit(r)


if __name__ == "__main__":
    main()
