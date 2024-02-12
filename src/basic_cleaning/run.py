#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args) -> None:
    logger.info("Starting wandb run for basic cleaning.")
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Using artifact: %s", args.input_artifact)
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(filepath_or_buffer=artifact_local_path)

    # Drop outliers
    logger.info(
        "Dropping outliers outside of $%.2f and $%.2f.", args.min_price, args.max_price
    )
    min_price = args.min_price
    max_price = args.max_price
    idx = df["price"].between(left=min_price, right=max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting 'last_review' column to datetime.")
    df["last_review"] = pd.to_datetime(arg=df["last_review"])

    df.to_csv(path_or_buf="clean_sample.csv", index=False)
    logger.info("Cleaned data saved to 'clean_sample.csv'.")

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(local_path="clean_sample.csv")
    run.log_artifact(artifact)
    logger.info(
        "Artifact %s of type %s logged.", args.output_artifact, args.output_type
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", type=str, help="Input artifact", required=True
    )

    parser.add_argument(
        "--output_artifact", type=str, help="Output artifact", required=True
    )

    parser.add_argument(
        "--output_type", type=str, help="Type of the output", required=True
    )

    parser.add_argument(
        "--output_description", type=str, help="Output Description", required=True
    )

    parser.add_argument("--min_price", type=int, help="Min price", required=True)

    parser.add_argument("--max_price", type=int, help="Max Price", required=True)

    args = parser.parse_args()

    go(args=args)
