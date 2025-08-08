from gfw.common.cli import CLI, Option
from src.migrate_dataset import migrate_dataset_command
from src.extract_dataset import extract_dataset_command
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
            migrate_dataset_command,
            extract_dataset_command,
        ],
        version='0.1.0',
        examples=[
            "migration_tools migrate_dataset --source_project p1"
            "--source_dataset d1 --dest_project p2 --dest_dataset d2",
            "migration_tools extract_dataset --source_project p1"
            "--source_dataset d1 --dest_project p2 --bucket_name test",
            "migration_tools extract_dataset --source_project p1"
            "--source_dataset d1 --dest_project p2 --bucket_name test --force_overwrite",
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
