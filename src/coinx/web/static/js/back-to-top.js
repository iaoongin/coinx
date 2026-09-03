(() => {
  const buttonSelector = '[data-back-to-top]';

  const scrollThreshold = 320;
  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

  const updateVisibility = () => {
    const button = document.querySelector(buttonSelector);
    if (!button) return;

    const shouldShow = window.scrollY > scrollThreshold;
    button.hidden = !shouldShow;
    button.setAttribute('aria-hidden', String(!shouldShow));
    button.tabIndex = shouldShow ? 0 : -1;
  };

  document.addEventListener('click', (event) => {
    const button = event.target.closest?.(buttonSelector);
    if (!button) return;

    window.scrollTo({
      top: 0,
      behavior: reducedMotion.matches ? 'auto' : 'smooth',
    });
  });

  updateVisibility();
  window.addEventListener('scroll', updateVisibility, { passive: true });
})();
