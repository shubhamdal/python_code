persona=[['09:00','09:30'],['10:30','11:00'],['13:00','14:00']]
personb=[['08:00','09:00'],['10:15','11:15']]

min_len = (len(persona) if len(persona) < len(personb) else len(personb))
max_len =  (len(persona) if len(persona) > len(personb) else len(personb))



rows, cols = (max_len-1, 2) 
merge_dates = [[0]*cols]*rows 
print(merge_dates) 

for i in range (0,min_len):
    for j in range(0,2):
        print(str(i)+" "+str(j))
        if persona[i][j]<personb[i][j]:
#             print("persona[i][j]"+str(persona[i][j]))
            merge_dates[i][j]=persona[i][j]
            print("if merge_dates["+str(i)+"]["+str(j)+"]"+str(merge_dates[i][j]))                             
        else:
#             print("personb[i][j]"+str(personb[i][j]))
            merge_dates[i][j]=personb[i][j]
            print("else merge_dates["+str(i)+"]["+str(j)+"]"+str(merge_dates[i][j]))
        
        
print("\n\n")        
print(str(merge_dates))
