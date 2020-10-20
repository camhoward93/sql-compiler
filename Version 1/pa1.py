#! /usr/bin/env python3

# Author's Notes ##############################################################
'''
Program: pa1
Language: Python 3
Author: Cameron Howard
Published Date: 02/21/19

This program is a database creation program that simulates some SQL commands.

For future versions color may be added to the errors.
Sample code:
		print("\033[1;31;40m !Failed", "\033[0;0m to create database", db_name,
			  		  "because it already exists.")
'''

# Imports #####################################################################
import os
import sys
import re
from shutil import rmtree

# Globals #####################################################################
# regular expressions
create_db_p = re.compile(r"CREATE DATABASE (\w+);\s*")
drop_db_p = re.compile(r"DROP DATABASE (\w+);\s*")
use_p = re.compile(r"USE (\w+);\s*")
create_tbl_p = re.compile(r"CREATE TABLE (\w+) \((.*\))\);\s*")
drop_tbl_p = re.compile(r"DROP TABLE (\w+);\s*")
select_p = re.compile(r"SELECT \* FROM (\w+);\s*")
alter_p = re.compile(r"ALTER TABLE (\w+) (\w+) (.*);\s*")
exit_p = re.compile(r"\.EXIT\s*")
whitespace_p = re.compile(r"\s*")

# USE flag
use_flag = False 

# Function Declarations #######################################################
def create_database(re_match):
	db_name = re_match.group(1)

	# check if database already exists
	if not os.path.exists(db_name):
		# make the database
		os.mkdir(db_name)

		# print success
		print("Database", db_name,  "created.")

	else:
		# print error
		print("!Failed to create database", db_name,
			  "because it already exists.")


def drop_database(re_match):
	global use_flag
	db_name = re_match.group(1)

	# make sure the database exists
	if (use_flag == True) and (db_name == os.getcwd()):
		os.chdir("..")
		use_flag = False

	if os.path.exists(db_name):
		# drop the database
		rmtree(db_name)

		# print success
		print("Database", db_name,  "deleted.")

	else:
		# print error
		print("!Failed to delete", db_name,
			  "because it does not exist.")


def use_database(re_match):
	global use_flag
	db_name = re_match.group(1)

	# make sure the database exists
	if use_flag == True:
		os.chdir("..")
		use_flag = False

	if os.path.exists(db_name):
		# change to the database
		os.chdir(db_name)
		use_flag = True

		# print success
		print("Using database", db_name)

	else:
		# print error
		print("!Failed to use database", db_name,
			  "because it does not exist.")


def create_table(re_match):
	global use_flag
	tbl_name = re_match.group(1)
	tbl_items = re_match.group(2)

	# check if file exists
	if os.path.isfile(tbl_name):
		# print error
		print("!Failed to create table", tbl_name,
			  "because it already exists.")

	# check USE flag
	elif not(use_flag):
		print("!Failed to create table", tbl_name,
			  "because USE has not been called on a valid database.")

	else:
		# create the file
		f = open(tbl_name, "w+")

		# parse the items
		items = tbl_items.split(", ")

		# write in the table
		delim = ""
		for x in items:
			f.write(delim)
			f.write(x)
			
			delim = " | "

		# close the file
		f.close()

		# print success
		print("Table", tbl_name, "created.")


def drop_table(re_match):
	tbl_name = re_match.group(1)

	# make sure the table exists
	if os.path.isfile(tbl_name):
		# drop the table
		os.remove(tbl_name)

		# print success
		print("Table", tbl_name,  "deleted.")

	else:
		# print error
		print("!Failed to delete table", tbl_name,
			  "because it does not exist.")


def select_from_table(re_match):
	tbl_name = re_match.group(1)

	# make sure the table exists
	if os.path.isfile(tbl_name):
		# open the file
		f = open(tbl_name, "r")

		# print the table
		print(f.read())

		# close the file
		f.close()

	else:
		# print error
		print("!Failed to query table", tbl_name,
			  "because it does not exist.")


def alter_table(re_match):
	tbl_name = re_match.group(1)
	tbl_cmd = re_match.group(2)
	tbl_items = re_match.group(3)

	# make sure the table exists
	if os.path.isfile(tbl_name):
		if(tbl_cmd == "ADD"):
			# open the file
			f = open(tbl_name, "a")

			# parse items
			items = tbl_items.split(", ")

			# append the table
			delim = " | "
			for x in items:
				f.write(delim)
				f.write(x)

			# print success
			print("Table", tbl_name, "modified.")

			# close the file
			f.close()

		else:
			print("Unknown alter command")

	else:
		# print error
		print("!Failed to modify table", tbl_name,
			  "because it does not exist.")


# Main Program ################################################################
# dictionary of commands and the corresponding function
commands = {create_db_p : create_database,
			drop_db_p : drop_database,
			use_p : use_database,
			create_tbl_p : create_table,
			drop_tbl_p : drop_table,
			select_p : select_from_table,
			alter_p : alter_table}

# run program until user exits
while True:

	# take in user input and set real_command flag
	input = sys.stdin.readline()

	# real command flag
	real_command = False

	# loop through possible commands and call the corresponding function
	for cmd in commands.items():
		m = cmd[0].fullmatch(input)
		if m is not None:
			cmd[1](m)
			real_command = True
			break

	# check for exit
	if not(real_command):
		m = exit_p.fullmatch(input)
		if m is not None:
			print("Program Terminated. Goodbye.")
			break

		# check for whitespace and ignore it
		m = whitespace_p.fullmatch(input)
		if m is not None:
			continue

		# not a real command
		print("!Failed to execute command. Check spelling and syntax.")

