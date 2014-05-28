class BinHeap:
	def __init__(self):
		self.heaplist = []
		self.size = 0
	def swap(self, a, b):
		tmp = self.heaplist[a]
		self.heaplist[a] = self.heaplist[b]
		self.heaplist[b] = tmp

	def printheap(self):
		print(self.heaplist)

	def add(self, val):
		self.heaplist.append(val)
		index = self.size
		while(index > 0):
			if self.heaplist[index] < self.heaplist[(index+1) / 2 - 1]:
				self.swap(index, (index+1) / 2 - 1)
			index = (index+1) / 2 - 1
		self.size += 1

	def remove(self):
		if (self.size == 0):
			return
		result = self.heaplist.pop(0)
		self.size -= 1
		self.heaplist.insert(0, self.heaplist.pop(self.size - 1))
		index = 0
		while (index + 1) * 2  - 1 <  self.size:
			if (index + 1) * 2 < self.size and self.heaplist[(index + 1) * 2  - 1] > self.heaplist[(index + 1) * 2]:
				min = (index + 1) * 2
			else:
				min = (index + 1) * 2  - 1

			if  self.heaplist[min] < self.heaplist[index]:
				self.swap(index, min)
				index = min
			else:
				break

	def peek(self):
		if(self.size == 0):
			return
		return self.heaplist[0]

	def isEmpty():
		return self.size == 0

if __name__ == "__main__":
	myheap = BinHeap()
	for i in range(10):
		myheap.add(10-i)
	myheap.printheap()
	print(myheap.peek())
	myheap.remove()
	myheap.printheap()
	print(myheap.peek())
	myheap.remove()
	myheap.printheap()
	print(myheap.peek())






