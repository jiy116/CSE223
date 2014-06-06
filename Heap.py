from clkPort import clkPort
import json
class BinHeap:
	def __init__(self):
		self.heaplist = []
		self.dic = {}
		self.size = 0
		self.priority = 1
	def swap(self, a, b):
		tmp = self.heaplist[a]
		self.heaplist[a] = self.heaplist[b]
		self.heaplist[b] = tmp

	def printheap(self):
		print(self.heaplist)

	def add(self, val):
		val['priority'] = self.priority
		self.priority += 1
		self.dic[json.dumps({'clock':val['log']['vClock'], 'id':val['log']['id']})] = val
		print "add:"
		print val
		self.heaplist.append(val)
		index = self.size
		while(index > 0):
			if self.heaplist[index]['priority'] >= self.heaplist[(index+1) / 2 - 1]['priority']:
				newer = self.clkCompare(self.heaplist[index]['log']['vClock'], self.heaplist[(index+1) / 2 - 1]['log']['vClock']) 
				if newer == -1:
					newer = self.heaplist[index]['log']['id'] > self.heaplist[(index+1) / 2 - 1]['log']['id']
				if not newer:
					self.swap(index, (index+1) / 2 - 1)
			else:
				self.swap(index, (index+1) / 2 - 1)
			index = (index+1) / 2 - 1
		self.size += 1

	def addi(self, val):
		self.dic[json.dumps({'clock':val['log']['vClock'], 'id':val['log']['id']})] = val
		print "add:"
		print val
		self.heaplist.append(val)
		index = self.size
		while(index > 0):
			if self.heaplist[index]['priority'] != self.heaplist[(index+1) / 2 - 1]['priority']:
				if self.heaplist[index]['priority'] < self.heaplist[(index+1) / 2 - 1]['priority']:
					self.swap(index, (index+1) / 2 - 1)
			else:
				newer = self.clkCompare(self.heaplist[index]['log']['vClock'], self.heaplist[(index+1) / 2 - 1]['log']['vClock']) 
				if newer == -1:
					newer = self.heaplist[index]['log']['id'] > self.heaplist[(index+1) / 2 - 1]['log']['id']
				if not newer:
					self.swap(index, (index+1) / 2 - 1)
			index = (index+1) / 2 - 1
		self.size += 1

	def remove(self):
		if (self.size == 0):
			return
		del self.dic[json.dumps({'clock':self.heaplist[0]['log']['vClock'],'id':self.heaplist[0]['log']['id']})]
		result = self.heaplist.pop(0)
		print "remove:"
		print result
		self.size -= 1
		if (self.size == 0):
			return
		self.heaplist.insert(0, self.heaplist.pop(self.size - 1))
		index = 0
		while (index + 1) * 2  - 1 <  self.size:
			if (index + 1) * 2 < self.size and self.heaplist[(index + 1) * 2  - 1] > self.heaplist[(index + 1) * 2]:
				min = (index + 1) * 2
			else:
				min = (index + 1) * 2  - 1
			if self.heaplist[min]['priority'] != self.heaplist[index]['priority']:
				if self.heaplist[min]['priority'] > self.heaplist[index]['priority']:
					break
				else:
					self.swap(index, min)
			else:
				newer = self.clkCompare(self.heaplist[min]['log']['vClock'], self.heaplist[index]['log']['vClock'])
				if newer == -1:
					newer = self.heaplist[min]['log']['id'] > self.heaplist[index]['log']['id']
				if  not newer:
					self.swap(index, min)
					index = min
				else:
					break

	def percDown(self,index):
	    while (index + 1) * 2  - 1 <  self.size:
			if (index + 1) * 2 < self.size and self.heaplist[(index + 1) * 2  - 1] > self.heaplist[(index + 1) * 2]:
				min = (index + 1) * 2
			else:
				min = (index + 1) * 2  - 1
			if self.heaplist[min]['priority'] != self.heaplist[index]['priority']:
				if self.heaplist[min]['priority'] > self.heaplist[index]['priority']:
					break
				else:
					self.swap(index, min)
			else:
				newer = self.clkCompare(self.heaplist[min]['log']['vClock'], self.heaplist[index]['log']['vClock'])
				if newer == -1:
					newer = self.heaplist[min]['log']['id'] > self.heaplist[index]['log']['id']
				if  not newer:
					self.swap(index, min)
					index = min
				else:
					break

	def updatePriority(self, vClock, port, priority):#target is a dic {'log':log, 'priority':num}
		isFound = False
		for val in self.heaplist:
			if (val['log']['vClock'] == vClock) and (val['log']['id'] == port): 
				isFound = True
				index = self.heaplist.index(val)
				self.heaplist.remove(val)
				self.size -= 1
				if len(self.heaplist) > 0:
					self.heaplist.insert(index, self.heaplist.pop())
					# print self.heaplist
					self.percDown(index)
					print self.size
				val['priority'] = priority
				self.addi(val)
				#set priority to a bigger one
				if self.priority < priority:
					self.priority = priority
		if not isFound:
			print "cannot find the target log"

	def getPriority(self):
		return self.priority

	def isEqual(self, val, target):
		if val['log'] == target['log']:
			# if val['priority'] == target['priority']:
				return True
		return False

	def peek(self):
		if self.size == 0:
			return
		#print("peek")
		#print self.heaplist[0]
		return self.heaplist[0]['log']

	def isEmpty(self):
		return self.size == 0

	def isDeliverable(self):
		if self.size != 0:
			return self.heaplist[0]['log']['deliverable'] == True
		else:
			return False

	def setDeliverable(self,clkport):
		if self.size == 0:
			return
		self.dic[clkport]['log']['deliverable'] = True

	def clkCompare(self, lista, listb):
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
	log = {'vClock': [1,2,3], 'id': 3}
	dic = {'log': log, 'priority': 5}
	myheap.add(dic)
	log = {'vClock': [2,2,3], 'id': 4}
	dic = {'log': log, 'priority': 5}
	myheap.add(dic)
	log = {'vClock': [3,2,3], 'id': 5}
	dic = {'log': log, 'priority': 5}
	myheap.add(dic)
	log = {'vClock': [6,1,3], 'id': 6}
	dic = {'log': log, 'priority': 5}
	myheap.add(dic)
	log = {'vClock': [6,1,3], 'id': 7}
	dic = {'log': log, 'priority': 5}
	myheap.add(dic)
	# myheap.printheap()
	# log = {'vClock': [3,2,3], 'id': 5}
	# dic = {'log': log, 'priority': 3}
	# myheap.add(dic)
	# log = {'vClock': [1,2,3], 'id': 7}
	# dic = {'log': log, 'priority': 3}
	# myheap.add(dic)

	myheap.printheap()
	log = {'vClock': [3,2,3], 'id': 5}
	dic1 = {'log': log, 'priority':2 }
	myheap.updatePriority(dic1['log']['vClock'],dic1['log']['id'],3)
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	myheap.remove()
	myheap.printheap()
	# myheap.remove()
	# 	# for i in range(10):
# 	# 	myheap.add(10-i)
# 	# myheap.printheap()
# 	# myheap.remove()
# 	# myheap.printheap()
# 	# myheap.remove()
# 	# myheap.printheap()