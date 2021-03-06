#! /usr/bin/env python3

# Author's Notes ######################################################################################################
'''
Program: pa2
Language: Python 3
Author: Cameron Howard
Published Date: 4/16/19

This program is a database creation program that simulates some SQL commands.

Updates from pa1:
- Regular expressions for SQL commands are now case insensitive
- Table structures updated to accomodate insert's spacing and new lines
- Select function updated to allow multi-line commands
- Helper functions added for select
- Modified USE flag checking throughout several functions
- Fixed whitespace handling in regular expressions
- Alter function disabled:
  Replaced with Delete and Update funcions

Notes:
- Currently no overflow check on multi-line functions for if there is no semicolon
- Table names are case sensitive
- May want to use a try except around the parameter_index in update and select functions

For future versions:
- List comprehesions should replace several loops
- New functions need to be implemented to replace repetetive code
- Color may be added to the errors.
	Sample code:

		print("\033[1;31;40m !Failed", "\033[0;0m to create database", db_name,
			  		  "because it already exists.")
'''

# Imports #############################################################################################################
import os
import sys
import re
from shutil import rmtree
import operator
import time

# Globals #############################################################################################################
# regular expressions
create_db_p = re.compile(r"CREATE\s+DATABASE\s+(\w+);\s*", re.I)
drop_db_p = re.compile(r"DROP\s+DATABASE\s+(\w+);\s*", re.I)
use_p = re.compile(r"USE\s+(\w+);\s*", re.I)
create_tbl_p = re.compile(r"CREATE\s+TABLE\s+(\w+)\s+\((.*)\);\s*", re.I)
drop_tbl_p = re.compile(r"DROP\s+TABLE\s+(\w+);\s*", re.I)
insert_p = re.compile(r"INSERT\s+INTO\s+(\w+)\s+values\((.*)\);\s*", re.I)
update_p = re.compile("UPDATE\s+.*", re.I|re.S)
delete_p = re.compile("DELETE\s.*", re.I|re.S)
select_p = re.compile("SELECT\s+.*", re.I|re.S)
alter_p = re.compile(r"ALTER\s+TABLE\s+(\w+)\s+(\w+)\s+(.*);\s*", re.I)
exit_p = re.compile(r"\.EXIT\s*", re.I)
whitespace_p = re.compile(r"\s*")

# use flag
use_flag = False

# dictionaries
ops = { "!=" : operator.ne,
		"=" : operator.eq,
		"<" : operator.lt,
		"<=" : operator.le,
		">" : operator.gt,
		">=" : operator.ge 	}

casts = { 	"varchar(20)" : str,
			"int" : int,
			"float" : float	}


# Function Declarations ###############################################################################################
# Create Database Function
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


# Delete Database Function
def drop_database(re_match):
	global use_flag
	db_name = re_match.group(1)

	# make sure the database exists
	if (use_flag == True) and (db_name == os.path.basename(os.getcwd())):
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


# Use Database Function
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
		print("Using database", db_name + ".")

	else:
		# print error
		print("!Failed to use database", db_name,
			  "because it does not exist.")


# Create Table Function
def create_table(re_match):
	global use_flag
	global casts
	tbl_name = re_match.group(1)
	tbl_items = re_match.group(2)

	# check USE flag
	if not(use_flag):
		print("!Failed to create table", tbl_name,
			  "because USE has not been called on a valid database.")

	# check if file exists
	elif os.path.isfile(tbl_name):
		# print error
		print("!Failed to create table", tbl_name,
			  "because it already exists.")

	else:
		# create the file
		f = open(tbl_name, "w+")

		# parse the items
		items = tbl_items.split(", ")

		# check for valid data types
		dcheck = [i.split(" ")[1] for i in items]
		dtypes = [i[0] for i in casts.items()]

		if set(dcheck).issubset(dtypes):
			# write in the table
			delim = ""
			for x in items:
				f.write(delim)
				f.write(x)
				
				delim = "|"

			f.write("\n")

			# close the file
			f.close()

			# print success
			print("Table", tbl_name, "created.")

		else:
			print("Table", tbl_name, "not created because datatypes are invalid.")


# Delete Table Function
def drop_table(re_match):
	tbl_name = re_match.group(1)

	# check USE flag
	if not(use_flag):
		print("!Failed to delete table", tbl_name,
			  "because USE has not been called on a valid database.")

	# make sure the table exists
	elif os.path.isfile(tbl_name):
		# drop the table
		os.remove(tbl_name)

		# print success
		print("Table", tbl_name,  "deleted.")

	else:
		# print error
		print("!Failed to delete table", tbl_name,
			  "because it does not exist.")


# Insert Value to Table Function
def insert_to_table(re_match):
	tbl_name = re_match.group(1)
	tbl_items = re_match.group(2)

	# make sure the table exists
	if os.path.isfile(tbl_name):
		# open the file
		f = open(tbl_name, "a")

		# parse items
		items = tbl_items.split(",")
		items = [i.strip().replace("'", "") for i in items]

		# append the table
		delim = ""
		for x in items:
			f.write(delim)
			f.write(x)
			delim = "|"

		f.write("\n")

		# print success
		print("1 new record inserted.")

		# close the file
		f.close()

	else:
		# print error
		print("!Failed to insert into table", tbl_name,
			  "because it does not exist.")


# Update Table Function
def update_table(re_match):
	# declare variables
	global use_flag
	semico_p = re.compile(r".*;\s*", re.S)
	input = re_match.group(0)
	update_param_p = re.compile(r"UPDATE\s+(.+)\s+SET\s+(.+)\s+WHERE\s+(.+);\s*", re.I)

	# take in the update command all the way to the semicolon
	while True:
		re_match = semico_p.fullmatch(input)

		if re_match is None:
			input += sys.stdin.readline()

		else:
			break

	# normalize the string
	input = input.replace("\n", "")

	# parse the string for desired information and execute
	u_param = update_param_p.fullmatch(input)

	# check USE flag
	if not(use_flag):
		print("!Failed update. USE has not been called on a valid database.")

	elif u_param is not None:
		# make sure the table exists
		tbl_name = u_param.group(1)
		if os.path.isfile(tbl_name):
			# open the file and read the lines
			with open(tbl_name, "r+") as f:
				#read = table.readlines()
				
				# loop through the lines and update where needed

				# read the table header and parse the elements
				lines = f.readlines()
				headers = lines[0].split("|")
				headers[-1] = headers[-1].rstrip()
				dtypes = list()
				delimiter = "|"

				# strip and store the data types
				for i in range(len(headers)):
					parsed = headers[i].split(" ")
					headers[i] = parsed[0]
					dtypes.append(parsed[1])

				# parse the request
				request = u_param.group(2).split(" ")
				request =[i.replace("'", "") for i in request]
				request[-1] = request[-1].rstrip()

				# parse the parameter
				parameter = u_param.group(3).split(" ")
				parameter =[i.replace("'", "") for i in parameter]

				# check the request and parameter
				if (
						set(request).intersection(headers) and
						set(parameter).intersection(headers)
					):
					# assign the indices of desired table columns to list
					set_index = headers.index(request[0])
					set_val = request[2]
					param_index = headers.index(parameter[0])
					castval = dtypes[param_index]
					rewrite = list()
					rewrite.append(lines[0])
					num_changes = 0

					# until end of file
					for line in lines[1:]:
						# split and strip line
						rvalues = line.split("|")

						# check the parameter
						if evaluate(parameter, rvalues[param_index], castval):
							# update requested value
							rvalues[set_index] = set_val


							# write to file
							line = delimiter.join(rvalues)

							# update counter
							num_changes += 1

						line = line.strip()
						line += "\n"
						rewrite.append(line)

					f.seek(0,0)

					for x in rewrite:
						f.write(x)

					f.truncate()

					# report success
					if(num_changes == 1):
						print("1 record modified.")

					else:
						print(num_changes, "records modified.")

				# report failure
				else:
					print("!Failed to update table", tbl_name,
						"because requested change is invalid.")

		# report failure
		else:
			print("!Failed to update table", tbl_name,
			  "because it does not exist.")

	else:
		# print error
		print("Failed! to update. Select command syntax invalid.")


# Delete Tuple Function
def delete_from_table(re_match):
	# declare variables
	global use_flag
	semico_p = re.compile(r".*;\s*", re.S)
	input = re_match.group(0)
	delete_param_p = re.compile(r"DELETE\s+FROM\s+(.+)\s+WHERE\s+(.+);\s*", re.I)

	# take in the update command all the way to the semicolon
	while True:
		re_match = semico_p.fullmatch(input)

		if re_match is None:
			input += sys.stdin.readline()

		else:
			break

	# normalize the string
	input = input.replace("\n", "")

	# parse the string for desired information and execute
	d_param = delete_param_p.fullmatch(input)

	# check USE flag
	if not(use_flag):
		print("!Failed update. USE has not been called on a valid database.")

	elif d_param is not None:
		# make sure the table exists
		tbl_name = d_param.group(1)
		if os.path.isfile(tbl_name):
			# open the file and read the lines
			with open(tbl_name, "r+") as f:
				#read = table.readlines()
				
				# loop through the lines and update where needed

				# read the table header and parse the elements
				lines = f.readlines()
				headers = lines[0].split("|")
				headers[-1] = headers[-1].rstrip()
				dtypes = list()
				delimiter = "|"

				# strip and store the data types
				for i in range(len(headers)):
					parsed = headers[i].split(" ")
					headers[i] = parsed[0]
					dtypes.append(parsed[1])

				# parse the parameter
				parameter = d_param.group(2).split(" ")
				parameter =[i.replace("'", "") for i in parameter]

				# check the request and parameter
				if (set(parameter).intersection(headers)):
					# assign the indices of desired table columns to list
					param_index = headers.index(parameter[0])
					castval = dtypes[param_index]
					rewrite = list()
					rewrite.append(lines[0])
					num_changes = 0

					# until end of file
					for line in lines[1:]:
						# split and strip line
						rvalues = line.split("|")

						# check the parameter
						if evaluate(parameter, rvalues[param_index], castval):
							# delete
							line = ''

							# update counter
							num_changes += 1

						rewrite.append(line)

					f.seek(0,0)

					for x in rewrite:
						f.write(x)

					f.truncate()

					# report success
					if(num_changes == 1):
						print("1 record deleted.")

					else:
						print(num_changes, "records deleted.")

				# report failure
				else:
					print("!Failed to delete from table", tbl_name,
						"because requested delete is invalid.")

		# report failure
		else:
			print("!Failed to delete from table", tbl_name,
			  "because it does not exist.")

	else:
		# print error
		print("Failed! to delete. Select command syntax invalid.")


# Select Function
def select_from_table(re_match):
	# declare variables
	global use_flag
	semico_p = re.compile(r".*;\s*", re.S)
	input = re_match.group(0)

	# command checks
	select_all_p = re.compile(r"SELECT\s+\*\s+FROM\s+(\w+);\s*", re.I)
	select_no_param_p = re.compile(r"SELECT\s+(.+)\s+FROM\s+(\w+);\s*", re.I)
	select_param_p = re.compile(r"SELECT\s+(.+)\s+FROM\s+(\w+)\s+WHERE\s+(.+);\s*", re.I)

	# take in the select command all the way to the semicolon
	while True:
		re_match = semico_p.fullmatch(input)

		if re_match is None:
			input += sys.stdin.readline()

		else:
			break

	# normalize the string
	input = input.replace("\n", "")

	# parse the string for desired information and execute
	s_all = select_all_p.fullmatch(input)
	s_no_param = select_no_param_p.fullmatch(input)
	s_param = select_param_p.fullmatch(input)

	# check USE flag
	if not(use_flag):
		print("!Failed query. USE has not been called on a valid database.")

	# select all
	elif s_all is not None:
		sel_all(s_all)

	# select without parameters
	elif s_no_param is not None:
		sel_no_p(s_no_param)

	elif s_param is not None:
		sel_p(s_param)

	else:
		# print error
		print("Failed! to query. Select command syntax invalid.")


# Select Helper Functions
# Select All (Helper of Select)
def sel_all(re_match):
	# make sure the table exists
	tbl_name = re_match.group(1)
	if os.path.isfile(tbl_name):
		# open the file
		f = open(tbl_name, "r")

		# print the table
		print(f.read(), end='')

		# close the file
		f.close()

	# report failure
	else:
		print("!Failed to query table", tbl_name,
		  "because it does not exist.")


# Select Without Parameters (Helper of Select)
def sel_no_p(re_match):
	# make sure the table exists
	tbl_name = re_match.group(2)
	if os.path.isfile(tbl_name):
		# open the file
		f = open(tbl_name, "r")

		# read the table header and parse the elements
		line = f.readline()
		headers = line.split("|")
		headers[-1] = headers[-1].rstrip()
		pheaders = headers[:]

		# strip the data types
		for i in range(len(headers)):
			parsed = headers[i].split(" ")
			headers[i] = parsed[0]

		# parse the requested values
		values = re_match.group(1).split(", ")
		values[-1] = values[-1].rstrip()

		# check the values
		if set(values).issubset(headers):
			# assign the indices of desired table columns to list
			indices = list()
			delimiter = ""

			for i in values:
				if i in headers:
					indices.append(headers.index(i))

			# print requested columns
			# print headers
			for i in indices:
				print(delimiter + pheaders[i], end='')
				delimiter = "|"
			print()

			# until end of file
			for line in f:
				# reset delimiter
				delimiter = ""

				# split and strip line
				rvalues = line.split("|")
				rvalues[-1] = rvalues[-1].rstrip()
					
				# print only columns given by indices
				for i in indices:
					print(delimiter + rvalues[i], end='')
					delimiter = "|"
				print()

		# report failure
		else:
			print("!Failed to query table", tbl_name, "because requested values are invalid.")

		# close the file
		f.close()

	# report failure
	else:
		print("!Failed to query table", tbl_name, "because it does not exist.")


# Select With Parameters (Helper of Select)
def sel_p(re_match):
	# make sure the table exists
	tbl_name = re_match.group(2)
	if os.path.isfile(tbl_name):
		# open the file
		f = open(tbl_name, "r")

		# read the table header and parse the elements
		line = f.readline()
		headers = line.split("|")
		headers[-1] = headers[-1].rstrip()
		pheaders = headers[:]
		dtypes = list()

		# strip and store the data types
		for i in range(len(headers)):
			parsed = headers[i].split(" ")
			headers[i] = parsed[0]
			dtypes.append(parsed[1])

		# parse the requested values
		values = re_match.group(1).split(", ")
		values[-1] = values[-1].rstrip()

		# parse the parameter
		parameter = re_match.group(3).split(" ")

		# check the values
		if (
				set(values).issubset(headers) and
				set(parameter).intersection(headers)
			):
			# assign the indices of desired table columns to list
			indices = list()
			param_index = headers.index(parameter[0])
			castval = dtypes[param_index]
			delimiter = ""

			for i in values:
				if i in headers:
					indices.append(headers.index(i))

			# print requested columns
			# print headers
			for i in indices:
				print(delimiter + pheaders[i], end='')
				delimiter = "|"
			print()

			# until end of file
			for line in f:
				# split and strip line
				rvalues = line.split("|")
				rvalues[-1] = rvalues[-1].rstrip()

				# check the parameter
				if evaluate(parameter, rvalues[param_index], castval):
					# reset delimiter
					delimiter = ""

					# print only columns given by indices and parameter
					for i in indices:
						print(delimiter + rvalues[i], end='')
						delimiter = "|"
					print()

		# report failure
		else:
			print("!Failed to query table", tbl_name,
				"because requested values are invalid.")

		# close the file
		f.close()

	# report failure
	else:
		print("!Failed to query table", tbl_name,
		  "because it does not exist.")


# Select With Parameter Evaluation Function (Helper of Select and Update)
def evaluate(exp, data, cast):
	global ops
	global casts

	# extract desired pieces of expression
	checkval = exp[2]
	checkop = exp[1]

	# cast and use operator
	cast_func = casts[cast]
	a = cast_func(data)
	b = cast_func(checkval)
	op = ops[checkop]

	r = op(a,b)
	return r


# Alter Table - Currently Unused
def alter_table(re_match):
	pass


# Main Program ########################################################################################################
# dictionary of commands and the corresponding function
commands = {create_db_p : create_database,
			drop_db_p : drop_database,
			use_p : use_database,
			create_tbl_p : create_table,
			drop_tbl_p : drop_table,
			insert_p : insert_to_table,
			delete_p : delete_from_table,
			update_p : update_table,
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

	# check for exit, whitespace, or invalid command
	if not(real_command):
		m = exit_p.fullmatch(input)
		if m is not None:
			print("All done.")
			break

		# check for whitespace and ignore it
		m = whitespace_p.fullmatch(input)
		if m is not None:
			continue

		# not a real command
		print("!Failed to execute command. Check spelling and syntax.")

