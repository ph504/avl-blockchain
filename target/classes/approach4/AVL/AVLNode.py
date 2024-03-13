
from typing import TypeVar, Generic

K = TypeVar('K')
V = TypeVar('V')

class AVLNode(Generic[K, V]):
    def __init__(self, key: K, value: V, left=None, right=None):
        self.key = key
        self.value = value
        self.left = left
        self.right = right
        self.level = 0
        self.depth = 0

        if left is None and right is None:
            self.set_depth(1)
        elif left is None:
            self.set_depth(right.get_depth() + 1)
        elif right is None:
            self.set_depth(left.get_depth() + 1)
        else:
            self.set_depth(max(left.get_depth(), right.get_depth()) + 1)

    def get_key(self) -> K:
        return self.key

    def set_key(self, key: K):
        self.key = key

    def get_left(self) -> 'AVLNode[K, V]':
        return self.left

    def set_left(self, left: 'AVLNode[K, V]'):
        self.left = left

    def get_right(self) -> 'AVLNode[K, V]':
        return self.right

    def set_right(self, right: 'AVLNode[K, V]'):
        self.right = right

    def get_depth(self) -> int:
        return self.depth

    def set_depth(self, depth: int):
        self.depth = depth

    def __lt__(self, other: 'AVLNode[K, V]') -> bool:
        return self.key < other.key

    def __str__(self) -> str:
        return f"Level {self.level}: {self.key}"

    def get_value(self) -> V:
        return self.value
