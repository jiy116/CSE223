from clkPort import clkPort
class BinHeap:
	def __init__(self):
		self.heaplist = []
		self.dic = {}
		self.size = 0
	def swap(self, a, b):
		tmp = self.heaplist[a]
		self.heaplist[a] = self.heaplist[b]
		self.heaplist[b] = tmp

	def printheap(self):
		print(self.heaplist)

	def add(self, val):
		clk = clkPort(val['vClock'], val['id'])
		self.dic[clk] = val
		self.heaplist.append(val)
		index = self.size
		while(index > 0):
			newer = clkCompare(self.heaplist[index]['vClock'], self.heaplist[(index+1) / 2 - 1]['vClock']) 
			if newer == -1:
				newer = self.heaplist[index]['id'] > self.heaplist[(index+1) / 2 - 1]['id']
			if not newer:
				self.swap(index, (index+1) / 2 - 1)
			index = (index+1) / 2 - 1
		self.size += 1

	def remove(self):
		if (self.size == 0):
			return
		clk = clkPort(val['vClock'], val['id'])
		del self.dic[clk]
		result = self.heaplist.pop(0)
		self.size -= 1
		self.heaplist.insert(0, self.heaplist.pop(self.size - 1))
		index = 0
		while (index + 1) * 2  - 1 <  self.size:
			if (index + 1) * 2 < self.size and self.heaplist[(index + 1) * 2  - 1] > self.heaplist[(index + 1) * 2]:
				min = (index + 1) * 2
			else:
				min = (index + 1) * 2  - 1
			newer = self.clkCompare(self.heaplist[min]['vClock'], self.heaplist[index]['vClock'])
			if newer == -1:
				newer = self.heaplist[min]['id'] > self.heaplist[index]['id']
			if  not newer:
				self.swap(index, min)
				index = min
			else:
				break

	def peek(self):
		if (self.size == 0):
			return
		result = self.heaplist[0]

	def isEmpty(self):
		return self.size == 0

	def isDeliverable(self):
		if self.size != 0:
			return self.heaplist[0]['deliverable'] == True
		else:
			return False

	def setDeliverable(self,clkport):
		if self.size == 0:
			return
		self.dic[clkport]['deliverable'] = True

	def clkCompare(lista, listb):
		length = len(lista)
		if length != len(listb):
			return -1	
		# -1 for imcomparable, 1 for lista after listb, 0 for lista before listb
		i = 0 #index
		flag = 0 #to help in comparision
		for i in range(length): 
			if(lista[i] >= listb[i]):
				flag += 1;
			else: 
				break
		if flag == length:
			return 1
		flag = 0
		for i in range(length): 
			if(lista[i] <= listb[i]):
				flag -= 1;
			else: 
				break
		if flag == -1 * length:
			return 0
		return -1

if __name__ == "__main__":
	myheap = BinHeap()
	for i in range(10):
		myheap.add(10-i)
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()