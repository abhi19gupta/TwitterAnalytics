def func(input):

	l1 = input["list1"]
	l2 = input["list2"]
	for x in l2:
		if x not in l1:
			l1.append(x)

	ret = {}
	ret["l_out"] = l1
	return ret