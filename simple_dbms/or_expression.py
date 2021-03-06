from conditional_expression import ConditionalExpression

class OrExpression(ConditionalExpression, object):
    def __init__(self, left, right):
        super(OrExpression, self).__init__(left, right)

	def is_true(self):
		return self.get_left().is_true() or self.get_right().is_true()
