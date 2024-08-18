from collections import deque

test_dict = {
    "val": {
        "m_v1": 1,
        "m_v2": 2,
        "m_v3": 3,
        "m_v4": 4,
    },
    "window": deque(maxlen=5)
}

window = test_dict.get("window")

for i in range(15):
    window.append(i)

    print(window)
    print(min(window))
    print(window.index(min(window)))

test_window = window
test_window.append(19)
print(test_dict)