
# Definir la función parentesis
def parentesis(name, i=0):
    if i == len(name):
        return ""

    if i < len(name) // 2:
        return "(" + name[i] + parentesis(name, i + 1)

    elif i == len(name) // 2:
        if len(name) % 2 == 0:
            return "()" + name[i]+")"+ parentesis(name, i + 1)
        else:
            return "(" + name[i] + ")" + parentesis(name, i + 1)

    else:
        return name[i] + ")" + parentesis(name, i + 1)
