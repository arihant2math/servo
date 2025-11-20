/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::ops::Range;

use script_bindings::root::DomRoot;

use crate::clipboard_provider::EmbedderClipboardProvider;
use crate::dom::bindings::str::DOMString;
use crate::dom::event::Event;
use crate::dom::keyboardevent::KeyboardEvent;
use crate::textinput::{
    ClipboardEventReaction, InputType, IsComposing, KeyReaction, Lines, SelectionDirection,
    SelectionState, TextInput, UTF8Bytes,
};

/// High-level mutation description returned by `Editor::handle_*`.
///
/// The caller (i.e. `DocumentEventHandler`) is responsible for translating
/// these mutations into DOM changes, `beforeinput`/`input` events and visual
/// invalidations.
#[derive(Debug)]
pub enum EditorMutation {
    /// Text content has changed (possibly because the user typed,
    /// deleted, pasted, etc.)
    ///
    /// • `text` – optional *replacement* string.  When `None`, the caller
    ///            should fetch the up-to-date buffer through
    ///            `Editor::text_content()`.
    Input {
        text: Option<String>,
        is_composing: IsComposing,
        input_type: InputType,
    },
    /// Only the selection / caret position moved, no textual change.
    RedrawSelection,
}

/// Wrapper around [`TextInput`] specialised with Servo’s embedder clipboard.
pub struct Editor {
    text_input: TextInput<EmbedderClipboardProvider>,
}

impl Editor {
    /// Construct a new, empty, multiline editor.
    ///
    /// `contenteditable` blocks are multiline by default and do not impose
    /// maxlength / minlength restrictions.
    pub fn new(clipboard: EmbedderClipboardProvider) -> Self {
        Self {
            text_input: TextInput::new(
                Lines::Multiple,
                DOMString::new(),
                clipboard,
                /* max_length    */ None,
                /* min_length    */ None,
                /* selection_dir */ SelectionDirection::None,
            ),
        }
    }

    // Keyboard handling

    /// Process a DOM `KeyboardEvent`.
    pub fn handle_keydown(
        &mut self,
        _event: &Event,
        keyboard_event: &DomRoot<KeyboardEvent>,
    ) -> (bool, Option<EditorMutation>) {
        // Delegate to TextInput’s logic.
        let reaction = self.text_input.handle_keydown(keyboard_event);

        let mutation = match reaction {
            KeyReaction::TriggerDefaultAction | KeyReaction::Nothing => None,
            KeyReaction::RedrawSelection => Some(EditorMutation::RedrawSelection),
            KeyReaction::DispatchInput(text, is_composing, input_type) => {
                Some(EditorMutation::Input {
                    text,
                    is_composing,
                    input_type,
                })
            },
        };

        // Let the caller decide whether to call `preventDefault()` – we simply
        // signal if we produced a mutation.
        (mutation.is_some(), mutation)
    }

    // TODO: Clipboard

    // State queries

    /// Current selection / caret info so that layout can paint it.
    pub fn selection_state(&self) -> SelectionState {
        self.text_input.selection_state()
    }

    /// Sorted byte-offset range of the current selection / caret.
    ///
    /// When there is no selection, this is an empty range whose start and end
    /// both equal the caret position.
    pub fn sorted_selection_offsets_range(&self) -> Range<UTF8Bytes> {
        self.text_input.sorted_selection_offsets_range()
    }

    /// Plain-text contents of the backing buffer.
    pub fn text_content(&self) -> String {
        self.text_input.get_content().to_string()
    }

    /// Overwrite the whole buffer (used by undo / load / execCommand(“insertText”)).
    pub fn set_text_content(&mut self, text: &str) {
        self.text_input.set_content(DOMString::from(text));
    }
}
