from bs4 import BeautifulSoup


def auto_detect_selectors(html):
    soup = BeautifulSoup(html, "html.parser")

    # Step 1: Define important attributes dynamically
    important_attributes = ["class", "id", "name"]

    def build_selector(tag):
        selector = tag.name
        for attr in tag.attrs:
            if attr in important_attributes or attr.startswith("data-"):
                value = tag.attrs[attr]
                if attr == "class":
                    selector += "." + ".".join(value)
                elif attr == "id":
                    selector += f"#{value}"
                else:
                    selector += f'[{attr}="{value}"]'
        return selector

    # Step 2: Generate selectors
    css_selectors = []
    xpath_selectors = []

    for tag in soup.find_all(True):
        css_selectors.append(build_selector(tag))

        # XPath generation
        xpath_parts = []
        current = tag
        while current is not None and current.name is not None:
            part = current.name
            for attr in tag.attrs:
                if attr in important_attributes or attr.startswith("data-"):
                    value = tag.attrs[attr]
                    if attr == "class":
                        part += f'[contains(@class, "{value}")]'
                    elif attr == "id":
                        part += f'[@id="{value}"]'
                    else:
                        part += f'[@{attr}="{value}"]'
            xpath_parts.insert(0, part)
            current = current.parent
        xpath_selectors.append("/" + "/".join(xpath_parts))

    return {"css_selectors": css_selectors, "xpath_selectors": xpath_selectors}
