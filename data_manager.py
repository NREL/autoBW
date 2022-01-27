#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on January 19 2022.

Uses code from Feedstock Production Emissions to Air Model (FPEAM) Copyright
(c) 2018 Alliance for Sustainable Energy, LLC; Noah Fisher.
Builds on functionality in the FPEAM's Data.py.
Unmodified FPEAM code is available at https://github.com/NREL/fpeam.

@author: rhanes
"""
import sys
import pandas as pd


class Data(pd.DataFrame):
    """
    Data representation.

    Specific datasets are created as child classes with
    defined column names, data types, and backfilling values. Creating child
    classes removes the need to define column names etc when the classes are
    called to read data from files.
    """

    COLUMNS = []

    INDEX_COLUMNS = []

    def __init__(
        self,
        fpath=None,
        columns=None,
        sheet=None,
        backfill=True,
    ):
        """
        Store file IO information into self.

        Parameters
        ----------
        fpath
            Filepath location of data to be read in

        columns
            List of columns to backfill

        sheet
            Name of sheet to read in

        backfill
            Boolean flag: perform backfilling with datatype-specific value
        """
        _df = (
            pd.DataFrame({})
            if fpath is None
            else self.load(fpath=fpath, columns=columns, sheet=sheet)
        )

        super().__init__(data=_df)

        self.source = fpath

        _valid = self.validate()

        try:
            assert _valid is True
        except AssertionError:
            if fpath is not None:
                print(f"{__name__} failed validation")
                raise

        if backfill:
            for _column in self.COLUMNS:
                if _column["backfill"] is not None:
                    self.backfill(column=_column["name"], value=_column["backfill"])

    @staticmethod
    def load(fpath, columns, header=0, sheet=None):
        """
        Load data from a text file at <fpath>. Check and set column names.

        See pandas.read_table() help for additional arguments.

        Parameters
        ----------
        fpath: [string]
            file path to CSV file or SQLite database file

        columns: [dict]
            {name: type, ...}

        header: [int]
            0-based row index containing column names

        sheet: [str]
            Specify the name of the sheet to be read in.
            If no sheet name is provided, the first sheet is read.

        Returns
        -------
        DataFrame
        """
        try:
            _df = pd.read_excel(
                io=fpath,
                sheet_name=sheet,
                dtype=columns,
                usecols=columns.keys(),
                header=header,
            )
        except ValueError as _e:
            print(f"{fpath}, {sheet}")
            raise

        else:
            return _df

    def backfill(self, column, value=0):
        """
        Replace NaNs in <column> with <value>.

        Parameters
        ----------
        column: [string]
            Name of column with NaNs to be backfilled

        value: [any]
            Value for backfill

        Returns
        -------
        DataFrame with [column] backfilled with [value]
        """
        _dataset = str(type(self)).split("'")[1]

        _backfilled = False

        if not value:
            print(f"DataManager: No backfill value provided for {column}")
            sys.exit()

        if isinstance(column, str):
            if self[column].isna().any():
                # count the missing values
                _count_missing = sum(self[column].isna())
                # count the total values
                _count_total = self[column].__len__()

                # fill the missing values with specified value
                self[column].fillna(value, inplace=True)

                # log a warning with the number of missing values
                print(
                    f"{_count_missing} of {_count_total} data values in"
                    f" {_dataset}.{column} were backfilled as {value}"
                )

                _backfilled = True
            else:
                # log if no values are missing
                print(f"no missing data values in {_dataset}.{column}")

        elif isinstance(column, list):
            # if any values are missing,
            for _c in column:
                if self[_c].isna().any():
                    # count the missing values
                    _count_missing = sum(self[_c].isna())
                    # count the total values
                    _count_total = self[_c].__len__()

                    # fill the missing values with specified value
                    self[_c].fillna(value, inplace=True)

                    # log a warning with the number of missing values
                    print(
                        f"{_count_missing} of {_count_total} data values in"
                        f" {_dataset}.{_c} were backfilled as {value}"
                    )

                    _backfilled = True

                else:
                    # log if no values are missing
                    print(f"no missing data values in {_dataset}.{_c}")

        return self

    def validate(self):
        """
        Check that data are not empty.

        Return False if empty and True otherwise.

        Returns
        -------
        Boolean flag
        """
        _name = type(self).__name__

        _valid = True

        print(f"validating {_name}")

        if self.empty:
            print(f"no data provided for {_name}")
            _valid = False

        print(f"validated {_name}")

        return _valid

    def __enter__(self):
        """Return self."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Process exceptions."""
        # process exceptions
        if exc_type is not None:
            print(f"{exc_type}\n{exc_val}\n{exc_tb}")
            _out = False
        else:
            _out = self

        return _out


class CreateActivities(Data):
    """
    Read in and process the Create Activity data table.

    This data table enumerates activities to be created and added to a custom database.
    """

    COLUMNS = (
        {"name": "activity_database", "type": str, "index": False, "backfill": None},
        {"name": "activity", "type": str, "index": False, "backfill": None},
        {"name": "reference_product", "type": str, "index": False, "backfill": None},
        {
            "name": "reference_product_amount",
            "type": float,
            "index": False,
            "backfill": 1.0,
        },
        {
            "name": "reference_product_unit",
            "type": str,
            "index": False,
            "backfill": None,
        },
        {"name": "std_dev", "type": float, "index": False, "backfill": None},
        {"name": "activity_location", "type": str, "index": False, "backfill": None},
        {"name": "activity_version", "type": float, "index": False, "backfill": None},
        {"name": "code", "type": float, "index": False, "backfill": None},
    )

    def __init__(
        self,
        fpath=None,
        columns={d["name"]: d["type"] for d in COLUMNS},
        backfill=True,
    ):
        """Initialize Create Activities data frame."""
        super().__init__(
            fpath=fpath,
            columns=columns,
            sheet="Create Activities",
            backfill=backfill,
        )


class AddExchanges(Data):
    """
    Read in and process the Add Exchanges data table.

    This data table enumerated exchanges to be added to activities in a custom database.
    """

    COLUMNS = (
        {"name": "activity_database", "type": str, "index": True, "backfill": None},
        {"name": "exchange_database", "type": str, "index": False, "backfill": None},
        {"name": "activity", "type": str, "index": False, "backfill": None},
        {"name": "activity_code", "type": str, "index": False, "backfill": None},
        {"name": "activity_location", "type": str, "index": False, "backfill": None},
        {"name": "exchange", "type": str, "index": False, "backfill": None},
        {"name": "amount", "type": float, "index": False, "backfill": None},
        {"name": "unit", "type": str, "index": False, "backfill": None},
        {"name": "exchange_location", "type": str, "index": False, "backfill": None},
        {"name": "exchange_type", "type": str, "index": False, "backfill": None},
        {"name": "exchange_code", "type": str, "index": False, "backfill": None},
    )

    def __init__(
        self,
        fpath=None,
        columns={d["name"]: d["type"] for d in COLUMNS},
        backfill=True,
    ):
        """Initialize Add Exchanges data frame."""
        super().__init__(
            fpath=fpath,
            columns=columns,
            sheet="Add Exchanges",
            backfill=backfill,
        )


class CopyActivities(Data):
    """
    Read in and process the data table enumerating activities to be copied.

    Activities and their exchanges are copied from an existing database to a custom database.
    """

    COLUMNS = (
        {"name": "source_database", "type": str, "index": True, "backfill": None},
        {"name": "activity", "type": str, "index": False, "backfill": None},
        {"name": "activity_code", "type": str, "index": False, "backfill": None},
    )

    def __init__(
        self,
        fpath=None,
        columns={d["name"]: d["type"] for d in COLUMNS},
        backfill=True,
    ):
        """Initialize Copy Activities data frame."""
        super().__init__(
            fpath=fpath,
            columns=columns,
            sheet="Copy Activities",
            backfill=backfill,
        )


class DeleteExchanges(Data):
    """
    Read in and process the Delete Exchanges data table.

    This data table enumerated exchanges to be removed from a custom database.
    """

    COLUMNS = (
        {"name": "activity_database", "type": str, "index": True, "backfill": None},
        {"name": "activity", "type": str, "index": False, "backfill": None},
        {"name": "activity_code", "type": str, "index": False, "backfill": None},
        {"name": "exchange_database", "type": str, "index": False, "backfill": None},
        {"name": "exchange", "type": str, "index": False, "backfill": None},
        {"name": "exchange_code", "type": str, "index": False, "backfill": None},
    )

    def __init__(
        self,
        fpath=None,
        columns={d["name"]: d["type"] for d in COLUMNS},
        backfill=True,
    ):
        """Initialize Delete Exchanges data frame."""
        super().__init__(
            fpath=fpath,
            columns=columns,
            sheet="Delete Exchanges",
            backfill=backfill,
        )
