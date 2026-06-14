const FILE_ID_RE = /^df-[a-z0-9]{26}$/;

function ensureFieldStyles() {
  if (document.querySelector("style[data-datasette-file-field]")) {
    return;
  }
  const style = document.createElement("style");
  style.setAttribute("data-datasette-file-field", "");
  style.textContent = `
    .datasette-file-field {
      display: grid;
      gap: 8px;
    }
    .datasette-file-field-current {
      min-height: 2.5rem;
      border: 1px solid var(--rule, #ccc);
      border-radius: 5px;
      background: var(--paper, #eef6ff);
      padding: 8px 10px;
    }
    .datasette-file-field-empty,
    .datasette-file-field-raw {
      color: var(--muted, #666);
      font-size: 0.9em;
    }
    .datasette-file-field-raw code {
      color: var(--ink, #111);
      overflow-wrap: anywhere;
    }
    .datasette-file-field-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .datasette-file-field-picker[hidden] {
      display: none;
    }
    .datasette-file-field button {
      appearance: none;
      border: 1px solid var(--rule, #ccc);
      border-radius: 4px;
      background: #fff;
      color: var(--accent, #1a56db);
      cursor: pointer;
      font: inherit;
      font-size: 0.82rem;
      line-height: 1.2;
      padding: 7px 10px;
    }
    .datasette-file-field button:hover,
    .datasette-file-field button:focus {
      background: #f8fafc;
    }
    .datasette-file-field button:focus {
      outline: 3px solid rgba(26, 86, 219, 0.12);
      outline-offset: 1px;
    }
  `;
  document.head.appendChild(style);
}

function fileUrl(fileId) {
  return "/-/files/" + encodeURIComponent(fileId);
}

function setInputValue(input, value) {
  input.value = value || "";
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function renderCurrentFile(current, value) {
  current.textContent = "";
  if (!value) {
    const empty = document.createElement("span");
    empty.className = "datasette-file-field-empty";
    empty.textContent = "No file selected";
    current.appendChild(empty);
    return;
  }

  if (FILE_ID_RE.test(value)) {
    const file = document.createElement("datasette-file");
    file.setAttribute("file-id", value);
    const fallback = document.createElement("a");
    fallback.href = fileUrl(value);
    fallback.textContent = value;
    file.appendChild(fallback);
    current.appendChild(file);
    return;
  }

  const raw = document.createElement("span");
  raw.className = "datasette-file-field-raw";
  raw.appendChild(document.createTextNode("Current value: "));
  const code = document.createElement("code");
  code.textContent = value;
  raw.appendChild(code);
  current.appendChild(raw);
}

function renderFileField(node, field) {
  ensureFieldStyles();

  const input = field.input;
  input.type = "hidden";
  input.dataset.originalValueType = "null";
  input.setAttribute("aria-hidden", "true");

  const wrap = document.createElement("div");
  wrap.className = "datasette-file-field";
  wrap.setAttribute("role", "group");
  if (field.labelId) {
    wrap.setAttribute("aria-labelledby", field.labelId);
  }
  if (field.descriptionId) {
    wrap.setAttribute("aria-describedby", field.descriptionId);
  }

  const current = document.createElement("div");
  current.className = "datasette-file-field-current";
  renderCurrentFile(current, input.value);

  const actions = document.createElement("div");
  actions.className = "datasette-file-field-actions";

  const pickerWrap = document.createElement("div");
  pickerWrap.className = "datasette-file-field-picker";
  pickerWrap.id = field.id + "-file-picker";
  pickerWrap.hidden = true;
  let inlinePicker = null;
  let pickerOpening = false;

  function updateButtons() {
    chooseButton.textContent = input.value ? "Change file" : "Choose file";
    removeButton.hidden = !input.value;
  }

  function closeInlinePicker(restoreFocus) {
    if (inlinePicker) {
      inlinePicker.remove();
      inlinePicker = null;
    }
    pickerWrap.textContent = "";
    pickerWrap.hidden = true;
    chooseButton.setAttribute("aria-expanded", "false");
    if (restoreFocus) {
      chooseButton.focus();
    }
  }

  async function openInlinePicker() {
    if (inlinePicker) {
      closeInlinePicker(true);
      return;
    }
    if (pickerOpening) {
      return;
    }
    pickerOpening = true;
    try {
      const pickerUrl = new URL("./datasette-file-picker.js", import.meta.url);
      pickerUrl.search = new URL(import.meta.url).search;
      await import(pickerUrl.href);
      inlinePicker = document.createElement("datasette-file-picker");
      inlinePicker.setAttribute("mode", "inline");
      inlinePicker.setAttribute("column", field.context.column);
      if (input.value) {
        inlinePicker.setAttribute("current-file-id", input.value);
      }
      pickerWrap.textContent = "";
      pickerWrap.hidden = false;
      pickerWrap.appendChild(inlinePicker);
      chooseButton.setAttribute("aria-expanded", "true");
    } finally {
      pickerOpening = false;
    }

    const fileId = await inlinePicker.result;
    inlinePicker = null;
    pickerWrap.textContent = "";
    pickerWrap.hidden = true;
    chooseButton.setAttribute("aria-expanded", "false");

    if (fileId !== null && fileId !== input.value) {
      setInputValue(input, fileId);
      renderCurrentFile(current, input.value);
      updateButtons();
    }
    chooseButton.focus();
  }

  const chooseButton = document.createElement("button");
  chooseButton.type = "button";
  chooseButton.setAttribute("aria-controls", pickerWrap.id);
  chooseButton.setAttribute("aria-expanded", "false");
  chooseButton.addEventListener("click", openInlinePicker);
  actions.appendChild(chooseButton);

  const removeButton = document.createElement("button");
  removeButton.type = "button";
  removeButton.textContent = "Remove file";
  removeButton.addEventListener("click", () => {
    closeInlinePicker(false);
    setInputValue(input, "");
    renderCurrentFile(current, input.value);
    updateButtons();
    chooseButton.focus();
  });
  actions.appendChild(removeButton);
  updateButtons();

  node.appendChild(input);
  node.appendChild(wrap);
  wrap.appendChild(current);
  wrap.appendChild(actions);
  wrap.appendChild(pickerWrap);
}

document.addEventListener("datasette_init", function (event) {
  event.detail.registerPlugin("datasette-files", {
    version: "0.1",

    makeColumnField(context) {
      if (!context.columnType || context.columnType.type !== "file") {
        return null;
      }
      return {
        render: renderFileField,
        focus(node) {
          const button = node.querySelector("button:not([hidden])");
          if (button) {
            button.focus();
          }
        },
        destroy(node) {
          const picker = node.querySelector("datasette-file-picker");
          if (picker) {
            picker.remove();
          }
        },
      };
    },
  });
});
