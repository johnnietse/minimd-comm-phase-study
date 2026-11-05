def convert_decimal_to_bin(num):
    return bin(num)


def convert_for_real(num):
    result = ""

    if num == 0:
        return "0"
    
    while num > 0:
        remainder = num % 2
        result += str(remainder)
        num = num //2
    
    right_way = []
    for char in result:
        print(char)
        right_way.append(char)

    
    return right_way

print(f"{convert_decimal_to_bin(10)}")
print(f"{convert_for_real(10)}")

