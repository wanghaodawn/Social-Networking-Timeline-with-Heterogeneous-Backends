
import sys

def main():
	filename = "links.csv"
	f = open(filename, 'r')

	dic = {}

	while 1:
		line = str(f.readline()).strip()
		# line = line.strip()
		# If line is null
		if not line:
			break

		array = line.split(",")
		if len(array) != 2:
			continue

		key = array[1]
		if dic.has_key(key):
			value = dic.get(key)
			dic[key] = value + "!" + array[0]
		else:
			dic[key] = array[0]

	for key in sorted(dic):
		value = dic[key]
		print(key + "," + value)

if __name__ == "__main__":
	main()