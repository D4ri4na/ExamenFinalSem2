
def invert_string(s):
    # Caso base: si la cadena está vacía o tiene un solo carácter
    print(s)
    print (s[0])
    if len(s) <= 1:
        return s
    else:
        return invert_string(s[1:]) + s[0]

string = "rosa"
new_string = invert_string(string)
print(new_string)  


