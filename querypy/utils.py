def get_text_tree(node, indent: int = 0, verbose=False):
    """
    Prints a tree. Every node of the tree is expected to have a children
    attribute.

    Original tree printing code in Kotlin from: _How query engines work by Andy Groove_

    fun format(plan: LogicalPlan, indent: Int = 0): String {
      val b = StringBuilder()
      0.rangeTo(indent).forEach { b.append("\t") }
      b.append(plan.toString()).append("\n")
      plan.children().forEach { b.append(format(it, indent+1)) }
      return b.toString()
    }
    """
    output = ""
    output += "\t" * indent

    if verbose and hasattr(node, '__repr_verbose__'):
        output += node.__repr_verbose__()
    else:
        output += repr(node)
    output += "\n"
    for children in node.children():
        tree = get_text_tree(children, indent + 1, verbose)
        output += tree
    return output
